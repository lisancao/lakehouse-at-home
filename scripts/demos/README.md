# Demos

## What's here

| Suite | Purpose |
|-------|---------|
| [`showcase/`](showcase/README.md) | One-script-per-stack-component building blocks (Iceberg, UC, SDP, RTM, Airflow, Connect, K8s) |
| [`mlflow-agents/`](mlflow-agents/README.md) | Reference MLflow AI Gateway agents that manage the lakehouse |
| [`transformations/`](transformations/) + the rest of this file | 5-minute Spark Declarative Pipelines walkthrough with voiceover script |

---

# Spark Declarative Pipelines Demo

A 5-minute demo showcasing Spark 4.1's Declarative Pipelines feature.

## Prerequisites

Ensure the Spark 4.1 cluster is running:

```bash
./lakehouse start spark41
./lakehouse status
```

## Demo 1: Pipeline Basics (3 min)

### Commands

```bash
# 1. Initialize a new pipeline project
docker exec spark-master-41 /opt/spark/bin/spark-pipelines init --name my_pipeline

# 2. View generated structure
docker exec spark-master-41 ls -la my_pipeline/

# 3. View the config file
docker exec spark-master-41 cat my_pipeline/spark-pipeline.yml

# 4. View our demo pipeline code
cat scripts/demos/transformations/sales_pipeline.py

# 5. Dry-run to validate
docker exec spark-master-41 /opt/spark/bin/spark-pipelines dry-run --spec /scripts/demos/spark-pipeline.yml

# 6. Run the pipeline
docker exec spark-master-41 /opt/spark/bin/spark-pipelines run --spec /scripts/demos/spark-pipeline.yml
```

### Voiceover Script

---

**[INTRO - Before any commands]**

> "Today I want to show you Spark Declarative Pipelines, a new feature in Spark 4.1 that fundamentally changes how we build data pipelines. Instead of writing imperative code that tells Spark exactly how to orchestrate your data flows, you simply declare what your tables should contain, and the framework handles the rest."

---

**[PART 1: Run init command]**

> "Let's start by creating a new pipeline project. I'll run spark-pipelines init with a project name."

*Run: `docker exec spark-master-41 /opt/spark/bin/spark-pipelines init --name my_pipeline`*

> "The CLI scaffolds out a complete project structure for us. Let's see what it created."

---

**[PART 2: Run ls command]**

*Run: `docker exec spark-master-41 ls -la my_pipeline/`*

> "We get a clean project layout: a spark-pipeline.yml configuration file and a transformations directory where our pipeline code lives. This convention-over-configuration approach means every team structures their pipelines the same way."

---

**[PART 3: Show the config file]**

*Run: `docker exec spark-master-41 cat my_pipeline/spark-pipeline.yml`*

> "The configuration file is intentionally minimal. We define the pipeline name, which catalog and database to use, where to store pipeline state, and which Python files contain our transformations. That's it. No complex DAG definitions, no explicit dependency graphs. The framework infers all of that from your code."

---

**[PART 4: Show sales_pipeline.py]**

*Run: `cat scripts/demos/transformations/sales_pipeline.py`*

> "Now here's where it gets interesting. This is our actual pipeline code. Notice we're using decorators from pyspark.pipelines."
>
> "The first function, order_stream, uses the @sdp.table decorator. This creates a streaming table that ingests data from Kafka. We define the schema, configure the Kafka connection, and specify how to parse the JSON messages. The decorator handles all the streaming infrastructure - checkpointing, exactly-once semantics, failure recovery."
>
> "The second function, brand_summary, uses @sdp.materialized_view. This creates an aggregated view that joins our orders table with a brands dimension table and computes order totals per brand."
>
> "What's remarkable here is what we're NOT writing. There's no explicit read or write operations. No checkpoint management. No dependency ordering. No orchestration code at all. We simply declare what each table should contain, and the framework builds the execution graph automatically."

---

**[PART 5: Run dry-run]**

*Run: `docker exec spark-master-41 /opt/spark/bin/spark-pipelines dry-run --spec /scripts/demos/spark-pipeline.yml`*

> "Before we run anything, we validate with a dry-run. This is one of the most powerful features. The dry-run parses all our transformation code, resolves every table reference, validates the dependency graph, and checks for errors - all without processing a single byte of data."
>
> "This catches bugs that would traditionally only surface at runtime, after you've already spun up a cluster and started processing. In a production environment, this can save hours of debugging and significant compute costs."

---

**[PART 6: Run the pipeline]**

*Run: `docker exec spark-master-41 /opt/spark/bin/spark-pipelines run --spec /scripts/demos/spark-pipeline.yml`*

> "Now we execute for real. The framework determines the optimal execution order, manages all the streaming and batch processing, handles checkpoints, and monitors for failures."
>
> "What we've built here would traditionally require hundreds of lines of orchestration code - setting up streaming contexts, managing state, handling failures, coordinating dependencies. With declarative pipelines, we expressed the same logic in about 50 lines of pure business logic."

---

**[WRAP-UP]**

> "That's Spark Declarative Pipelines. Declare your tables. Validate before you run. Let the framework handle the complexity. It's a fundamentally simpler way to build production data pipelines."

---

## Demo 2: Catching Errors Early (2 min)

### Commands

```bash
# 1. Show the broken pipeline code
cat scripts/demos/transformations/sales_pipeline_broken.py

# 2. Try dry-run (will fail)
docker exec spark-master-41 /opt/spark/bin/spark-pipelines dry-run --spec /scripts/demos/spark-pipeline-broken.yml

# 3. Show the fix
echo "Fix: Change 'orderz' to 'orders' on line 44"

# 4. Run dry-run on fixed version
docker exec spark-master-41 /opt/spark/bin/spark-pipelines dry-run --spec /scripts/demos/spark-pipeline.yml
```

### Voiceover Script

---

**[INTRO]**

> "Now let me show you one of the most valuable features of declarative pipelines: catching errors before they cost you compute time and debugging hours."

---

**[PART 1: Show the broken pipeline]**

*Run: `cat scripts/demos/transformations/sales_pipeline_broken.py`*

> "Here's a pipeline that looks almost identical to our working version. But there's a subtle bug hiding in here. Look at line 44 - someone typed 'orderz' with a Z instead of 'orders'. It's the kind of typo that happens all the time, especially when you're working quickly or copying code."
>
> "In a traditional Spark job, this wouldn't fail at compile time. Python doesn't know that 'iceberg.bronze.orderz' doesn't exist. Your job would get submitted, a cluster would spin up, executors would be allocated, and only then - maybe minutes later, maybe longer if there's a queue - would you finally get a TABLE_NOT_FOUND error."
>
> "If this is part of a larger pipeline, you might have already processed gigabytes of data in upstream stages before hitting this failure. That's wasted compute. Wasted time. And often, wasted money."

---

**[PART 2: Run dry-run on broken version]**

*Run: `docker exec spark-master-41 /opt/spark/bin/spark-pipelines dry-run --spec /scripts/demos/spark-pipeline-broken.yml`*

> "Watch what happens when we run a dry-run on this broken pipeline."
>
> "Immediately - within seconds, before any cluster resources are allocated - the framework catches the error. TABLE_OR_VIEW_NOT_FOUND: the table 'iceberg.bronze.orderz' cannot be found. It tells us exactly which flow failed: gold.brand_summary. It even points us to the line in our code."
>
> "This is static analysis for data pipelines. The framework parses every transformation, resolves every table reference, and validates the entire dependency graph - all without executing any actual data processing."

---

**[PART 3: Show the fix]**

> "The fix is trivial once you know where to look. Line 44, change 'orderz' to 'orders'. One character."

*Show the diff:*
```
-    orders = spark.table("iceberg.bronze.orderz")
+    orders = spark.table("iceberg.bronze.orders")
```

---

**[PART 4: Run dry-run on fixed version]**

*Run: `docker exec spark-master-41 /opt/spark/bin/spark-pipelines dry-run --spec /scripts/demos/spark-pipeline.yml`*

> "Now let's dry-run the corrected version."
>
> "It passes. All table references resolve. All dependencies are valid. The pipeline graph is correct. Now we can run this pipeline with confidence, knowing that we won't hit a table-not-found error twenty minutes into execution."

---

**[WRAP-UP]**

> "This is the workflow I recommend for all production pipelines: always dry-run before you run. Make it part of your CI/CD. Make it part of your deployment process. Catch the bugs before they cost you."
>
> "Declarative pipelines give you compile-time safety for your data transformations. That's a game-changer for pipeline reliability."

---

## Key Takeaways

1. **Declarative over Imperative** - Define *what* tables should contain, not *how* to build them
2. **Automatic Dependency Resolution** - Framework determines execution order
3. **Validate Before Execute** - `dry-run` catches errors without compute cost
4. **Simple Configuration** - YAML spec + decorated Python functions

## File Reference

| File | Purpose |
|------|---------|
| `spark-pipeline.yml` | Working pipeline config |
| `spark-pipeline-broken.yml` | Broken config (for error demo) |
| `transformations/sales_pipeline.py` | Working pipeline code |
| `transformations/sales_pipeline_broken.py` | Code with typo (orderz) |
| `run_demo.sh` | Interactive demo script |
| `run_demo_fix_error.sh` | Interactive error-fix demo |

## Automated Demo Scripts

For a guided walkthrough with pauses:

```bash
# Full demo with init, dry-run, run
./scripts/demos/run_demo.sh

# Error catching demo
./scripts/demos/run_demo_fix_error.sh
```
