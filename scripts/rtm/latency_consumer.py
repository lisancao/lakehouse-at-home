#!/usr/bin/env python3
"""
Kafka consumer that reads from a topic containing events with produced_at
timestamps and computes end-to-end latency (consumer wall clock - produced_at).

Copied from spark-rtm-quickstart (the Spark 4.1 RTM quickstart). The latency
calculation is unchanged so 4.1 and 4.3 numbers are comparable:
  * wall clock taken once per poll() return, minus produced_at
  * p50 = sorted[int(n*0.50)], p95 = sorted[int(n*0.95)], p99 = sorted[min(int(n*0.99), n-1)]
  * stats are per reporting interval (default 10 s), then reset

Additions for the 4.3 runs:
  --duration N     stop after N seconds and print an overall summary
  --warmup N       ignore the first N seconds (query start-up)
  --json-out PATH  write per-interval and overall stats as JSON
  --id-field F     also count distinct values of field F (dedup check)

Usage:
    python latency_consumer.py [--bootstrap-servers localhost:9092] [--topic rtm-latency]
"""

import argparse
import json
import signal
import statistics
import time

from kafka import KafkaConsumer


def pct(sorted_lat):
    n = len(sorted_lat)
    return {
        "n": n,
        "p50": sorted_lat[int(n * 0.50)],
        "p95": sorted_lat[int(n * 0.95)],
        "p99": sorted_lat[min(int(n * 0.99), n - 1)],
        "mean": round(statistics.mean(sorted_lat), 1),
        "min": sorted_lat[0],
        "max": sorted_lat[-1],
    }


def main():
    parser = argparse.ArgumentParser(description="RTM latency stats consumer")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092")
    parser.add_argument("--topic", type=str, default="rtm-latency")
    parser.add_argument(
        "--interval",
        type=int,
        default=10,
        help="Reporting interval in seconds (default: 10)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=0,
        help="Stop after this many seconds (default: run until Ctrl+C)",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=0,
        help="Discard records seen in the first N seconds",
    )
    parser.add_argument("--json-out", type=str, default=None)
    parser.add_argument(
        "--id-field",
        type=str,
        default=None,
        help="Count distinct values of this JSON field",
    )
    args = parser.parse_args()

    # Use group_id=None for independent consumption (no group coordination delay)
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        auto_offset_reset="latest",
        group_id=None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    running = True

    def shutdown(_signum, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print(f"Consuming from {args.topic} (bootstrap: {args.bootstrap_servers})")
    print(f"Reporting latency stats every {args.interval} seconds")
    print("Press Ctrl+C to stop.\n", flush=True)

    latencies = []
    all_latencies = []
    intervals = []
    ids_seen = set()
    records_total = 0
    started = time.time()
    last_report = started

    while running:
        records = consumer.poll(timeout_ms=1000)
        now_ms = int(time.time() * 1000)
        in_warmup = time.time() - started < args.warmup

        for tp, messages in records.items():
            for msg in messages:
                if in_warmup:
                    continue
                records_total += 1
                if args.id_field:
                    ids_seen.add(msg.value.get(args.id_field))
                produced_at = msg.value.get("produced_at")
                if produced_at is not None:
                    latency_ms = now_ms - int(produced_at)
                    latencies.append(latency_ms)

        now = time.time()
        if now - last_report >= args.interval:
            if latencies:
                latencies.sort()
                s = pct(latencies)
                intervals.append(s)
                all_latencies.extend(latencies)
                print(
                    f"  [Latency] n={s['n']:,} | p50={s['p50']}ms | p95={s['p95']}ms | "
                    f"p99={s['p99']}ms | mean={s['mean']:.1f}ms | "
                    f"min={s['min']}ms | max={s['max']}ms",
                    flush=True,
                )
                latencies.clear()
            else:
                print("  [Latency] No data in this interval...", flush=True)
            last_report = now
        if args.duration and now - started >= args.duration:
            break

    consumer.close()
    all_latencies.extend(latencies)  # fold in the last, partial interval
    elapsed = time.time() - started - args.warmup
    summary = {
        "topic": args.topic,
        "records": records_total,
        "records_per_sec": round(records_total / elapsed, 1) if elapsed > 0 else None,
        "intervals": intervals,
    }
    if all_latencies:
        all_latencies.sort()
        summary["overall"] = pct(all_latencies)
        o = summary["overall"]
        print(
            f"\n  [Overall] n={o['n']:,} | p50={o['p50']}ms | p95={o['p95']}ms | "
            f"p99={o['p99']}ms | mean={o['mean']}ms | max={o['max']}ms | "
            f"{summary['records_per_sec']} rec/s"
        )
    if args.id_field:
        summary["distinct_ids"] = len(ids_seen)
        print(
            f"  [Distinct] {args.id_field}: {len(ids_seen):,} distinct of "
            f"{records_total:,} records"
        )
    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump(summary, f, indent=2)
    print("\nDone.")


if __name__ == "__main__":
    main()
