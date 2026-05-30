"""Order lifecycle event generator with realistic distributions."""

import json
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Generator, List, Tuple

import numpy as np

from .config import GeneratorConfig, Location
from .dimensions import BRANDS, get_items, Brand, Item


@dataclass
class Event:
    """A single event in the order lifecycle."""

    event_id: str
    event_type: str
    ts: datetime
    location_id: int
    order_id: str
    sequence: int
    body: dict


def generate_order_id() -> str:
    """Generate a 6-character alphanumeric order ID."""
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return "".join(random.choices(chars, k=6))


def gaussian_time(mean_mins: float, std_mins: float) -> float:
    """Generate a Gaussian-distributed time, minimum 0.5 minutes."""
    return max(0.5, np.random.normal(mean_mins, std_mins))


def interpolate_coords(
    start: Tuple[float, float],
    end: Tuple[float, float],
    progress: float,
) -> Tuple[float, float]:
    """Linear interpolation between two coordinates."""
    lat = start[0] + (end[0] - start[0]) * progress
    lon = start[1] + (end[1] - start[1]) * progress
    return (lat, lon)


def random_coords_in_location(location: Location) -> Tuple[float, float]:
    """Generate random coordinates within a location's bounding box."""
    lat = random.uniform(location.lat_range[0], location.lat_range[1])
    lon = random.uniform(location.lon_range[0], location.lon_range[1])
    return (lat, lon)


def select_brand(brands: List[Brand], day: int) -> Brand:
    """Select a brand weighted by momentum trajectory."""
    weights = []
    for brand in brands:
        base = 1.0
        if brand.momentum == "growing":
            # Grow 0.5% per day
            base *= (1.005 ** day)
        elif brand.momentum == "declining":
            # Decline 0.3% per day
            base *= (0.997 ** day)
        weights.append(base)

    total = sum(weights)
    weights = [w / total for w in weights]
    return random.choices(brands, weights=weights, k=1)[0]


def select_items_for_order(brand: Brand, all_items: List[Item]) -> List[dict]:
    """Select 1-5 items from a brand for an order."""
    brand_items = [i for i in all_items if i.brand_id == brand.id]
    if not brand_items:
        return []

    num_items = random.choices([1, 2, 3, 4, 5], weights=[0.3, 0.35, 0.2, 0.1, 0.05])[0]
    selected = random.choices(brand_items, k=min(num_items, len(brand_items)))

    return [
        {
            "item_id": item.id,
            "name": item.name,
            "price": item.price,
            "quantity": random.choices([1, 2, 3], weights=[0.7, 0.25, 0.05])[0],
        }
        for item in selected
    ]


def generate_order_events(
    order_time: datetime,
    location: Location,
    config: GeneratorConfig,
    day: int,
) -> List[Event]:
    """Generate all events for a single order lifecycle."""
    events = []
    order_id = generate_order_id()
    all_items = get_items()

    # Select brand and items
    brand = select_brand(BRANDS, day)
    items = select_items_for_order(brand, all_items)
    if not items:
        return []

    total = sum(i["price"] * i["quantity"] for i in items)

    # Customer and kitchen coordinates
    customer_coords = random_coords_in_location(location)
    kitchen_coords = (location.lat, location.lon)  # Kitchen at city center

    # Calculate route distance (simple haversine approximation)
    route_distance_km = np.sqrt(
        (customer_coords[0] - kitchen_coords[0]) ** 2 +
        (customer_coords[1] - kitchen_coords[1]) ** 2
    ) * 111  # Rough km per degree

    st = config.service_times
    seq = 0
    current_time = order_time

    # Decide whether this order is cancelled, and at which lifecycle stage.
    # Cancellations skew early (customer changes mind / kitchen rejects) and
    # terminate the lifecycle with an `order_cancelled` event.
    cancel_stage = None
    if random.random() < getattr(config, "cancel_rate", 0.0):
        cancel_stage = random.choices(
            ["created", "kitchen", "ready"], weights=[0.5, 0.35, 0.15])[0]

    _CANCEL_REASONS = {
        "created": ["customer_changed_mind", "payment_failed", "address_error"],
        "kitchen": ["item_out_of_stock", "kitchen_overloaded", "store_closing"],
        "ready": ["no_driver_available", "customer_unreachable"],
    }

    def _cancelled():
        nonlocal current_time
        current_time += timedelta(seconds=random.randint(20, 180))
        events.append(Event(
            event_id=str(uuid.uuid4()),
            event_type="order_cancelled",
            ts=current_time,
            location_id=location.id,
            order_id=order_id,
            sequence=seq,
            body={
                "cancelled_by": "customer" if cancel_stage == "created" else "kitchen",
                "reason": random.choice(_CANCEL_REASONS[cancel_stage]),
                "refund_total": round(total, 2),
                "brand_id": brand.id,
                "stage": cancel_stage,
            },
        ))
        return events

    # Event 1: order_created
    events.append(Event(
        event_id=str(uuid.uuid4()),
        event_type="order_created",
        ts=current_time,
        location_id=location.id,
        order_id=order_id,
        sequence=seq,
        body={
            "customer_lat": round(customer_coords[0], 6),
            "customer_lon": round(customer_coords[1], 6),
            "brand_id": brand.id,
            "brand_name": brand.name,
            "items": items,
            "total": round(total, 2),
        },
    ))
    seq += 1
    if cancel_stage == "created":
        return _cancelled()

    # Event 2: kitchen_started
    wait_mins = gaussian_time(*st.order_to_kitchen_start)
    current_time += timedelta(minutes=wait_mins)
    estimated_prep = brand.avg_prep_time_mins + random.randint(-3, 5)
    events.append(Event(
        event_id=str(uuid.uuid4()),
        event_type="kitchen_started",
        ts=current_time,
        location_id=location.id,
        order_id=order_id,
        sequence=seq,
        body={
            "kitchen_id": f"K{location.id:02d}",
            "estimated_prep_mins": estimated_prep,
        },
    ))
    seq += 1
    if cancel_stage == "kitchen":
        return _cancelled()

    # Event 3: kitchen_finished
    prep_mins = gaussian_time(*st.kitchen_prep)
    current_time += timedelta(minutes=prep_mins)
    events.append(Event(
        event_id=str(uuid.uuid4()),
        event_type="kitchen_finished",
        ts=current_time,
        location_id=location.id,
        order_id=order_id,
        sequence=seq,
        body={
            "actual_prep_mins": round(prep_mins, 1),
        },
    ))
    seq += 1

    # Event 4: order_ready
    current_time += timedelta(seconds=30)  # Quick packaging
    events.append(Event(
        event_id=str(uuid.uuid4()),
        event_type="order_ready",
        ts=current_time,
        location_id=location.id,
        order_id=order_id,
        sequence=seq,
        body={
            "pickup_lat": round(kitchen_coords[0], 6),
            "pickup_lon": round(kitchen_coords[1], 6),
        },
    ))
    seq += 1
    if cancel_stage == "ready":
        return _cancelled()

    # Event 5: driver_arrived
    driver_wait_mins = gaussian_time(*st.kitchen_to_driver)
    current_time += timedelta(minutes=driver_wait_mins)
    driver_id = f"D{random.randint(1, 500):04d}"
    vehicle_type = random.choice(["car", "bike", "scooter"])
    events.append(Event(
        event_id=str(uuid.uuid4()),
        event_type="driver_arrived",
        ts=current_time,
        location_id=location.id,
        order_id=order_id,
        sequence=seq,
        body={
            "driver_id": driver_id,
            "vehicle_type": vehicle_type,
        },
    ))
    seq += 1

    # Event 6: driver_picked_up
    current_time += timedelta(seconds=random.randint(30, 120))
    events.append(Event(
        event_id=str(uuid.uuid4()),
        event_type="driver_picked_up",
        ts=current_time,
        location_id=location.id,
        order_id=order_id,
        sequence=seq,
        body={
            "route_distance_km": round(route_distance_km, 2),
        },
    ))
    seq += 1
    pickup_time = current_time

    # Events 7-N: driver_ping (every 60 seconds during delivery)
    delivery_mins = gaussian_time(*st.pickup_to_delivery)
    delivery_duration = timedelta(minutes=delivery_mins)
    ping_interval = timedelta(seconds=st.driver_ping_interval_sec)

    elapsed = timedelta(0)
    while elapsed < delivery_duration - ping_interval:
        elapsed += ping_interval
        progress = elapsed / delivery_duration
        ping_coords = interpolate_coords(kitchen_coords, customer_coords, progress)

        current_time = pickup_time + elapsed
        events.append(Event(
            event_id=str(uuid.uuid4()),
            event_type="driver_ping",
            ts=current_time,
            location_id=location.id,
            order_id=order_id,
            sequence=seq,
            body={
                "lat": round(ping_coords[0], 6),
                "lon": round(ping_coords[1], 6),
                "progress_pct": round(progress * 100, 1),
            },
        ))
        seq += 1

    # Final event: delivered
    current_time = pickup_time + delivery_duration
    total_mins = (current_time - order_time).total_seconds() / 60
    events.append(Event(
        event_id=str(uuid.uuid4()),
        event_type="delivered",
        ts=current_time,
        location_id=location.id,
        order_id=order_id,
        sequence=seq,
        body={
            "delivery_lat": round(customer_coords[0], 6),
            "delivery_lon": round(customer_coords[1], 6),
            "total_mins": round(total_mins, 1),
        },
    ))

    return events


def get_orders_for_minute(
    minute_of_day: int,
    day_of_week: int,
    config: GeneratorConfig,
) -> int:
    """Get number of orders for a specific minute using Poisson distribution."""
    hour = minute_of_day // 60
    hour_weight = config.demand.hour_weights.get(hour, 0.5)
    day_mult = config.demand.day_multipliers.get(day_of_week, 1.0)

    # Base rate per minute
    daily_orders = config.base_orders_per_day * day_mult
    minute_rate = (daily_orders * hour_weight) / 60

    # Poisson arrival
    return np.random.poisson(minute_rate)


def generate_all_events(config: GeneratorConfig) -> Generator[Event, None, None]:
    """Generate all events for the configured time period."""
    np.random.seed(config.seed)
    random.seed(config.seed)

    start = datetime.combine(config.start_date, datetime.min.time())

    total_orders = 0
    total_events = 0

    for day in range(config.days):
        current_date = start + timedelta(days=day)
        day_of_week = current_date.weekday()

        for location in config.locations:
            for minute in range(1440):  # 24 * 60 minutes
                num_orders = get_orders_for_minute(minute, day_of_week, config)

                for _ in range(num_orders):
                    # Add random seconds within the minute
                    order_time = current_date + timedelta(
                        minutes=minute,
                        seconds=random.randint(0, 59),
                    )

                    events = generate_order_events(order_time, location, config, day)
                    total_orders += 1

                    for event in events:
                        total_events += 1
                        yield event

        # Progress logging
        if (day + 1) % 10 == 0:
            print(f"  Day {day + 1}/{config.days}: {total_orders:,} orders, {total_events:,} events")


def event_to_dict(event: Event) -> dict:
    """Convert an Event to a dictionary for serialization."""
    return {
        "event_id": event.event_id,
        "event_type": event.event_type,
        "ts": event.ts.isoformat(),
        "ts_seconds": int(event.ts.timestamp()),
        "location_id": event.location_id,
        "order_id": event.order_id,
        "sequence": event.sequence,
        "body": json.dumps(event.body),
    }
