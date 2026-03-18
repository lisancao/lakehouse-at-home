#!/usr/bin/env bash
# OverArchitected Act 5c: Spark on Kubernetes Reference
# =====================================================
#
# Reference commands for deploying Spark on K8s.
# This is a reference script — run commands individually, not as a batch.
#
# Prerequisites:
#   - minikube or kind installed
#   - kubectl configured
#   - Docker images available

set -euo pipefail

echo "============================================================"
echo "  ACT 5c: SPARK ON KUBERNETES"
echo "  Same image. Same code. Different orchestrator."
echo "============================================================"

# ─── Step 1: Start minikube ─────────────────────────────────
echo ""
echo "--- Step 1: Start minikube (if not running) ---"
echo ""
echo "  minikube start --cpus 4 --memory 8192 --driver=docker"
echo "  kubectl cluster-info"
echo ""

# ─── Step 2: Create RBAC for Spark ──────────────────────────
echo "--- Step 2: RBAC Setup ---"
echo ""
cat <<'RBAC'
  # Create service account
  kubectl create serviceaccount spark -n default

  # Grant permissions (pods, services, configmaps)
  kubectl create clusterrolebinding spark-role \
      --clusterrole=edit \
      --serviceaccount=default:spark \
      --namespace=default
RBAC
echo ""

# ─── Step 3: Test with SparkPi ──────────────────────────────
echo "--- Step 3: Test with SparkPi ---"
echo ""
cat <<'SPARKPI'
  # Get the API server URL
  API_SERVER=$(kubectl cluster-info | grep "control plane" | awk '{print $NF}')

  # Submit SparkPi
  spark-submit \
      --master k8s://$API_SERVER \
      --deploy-mode cluster \
      --name spark-pi \
      --conf spark.kubernetes.container.image=apache/spark:4.1.0-scala2.13-java21-python3-r-ubuntu \
      --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
      --conf spark.executor.instances=2 \
      --conf spark.executor.memory=1g \
      --conf spark.executor.cores=1 \
      local:///opt/spark/examples/src/main/python/pi.py

  # Watch pods spin up
  kubectl get pods -w
SPARKPI
echo ""

# ─── Step 4: Full Lakehouse on K8s ──────────────────────────
echo "--- Step 4: Full Lakehouse Spark Job on K8s ---"
echo ""
cat <<'LAKEHOUSE'
  # Custom image with JARs baked in (recommended for production)
  # See: scripts/tools/build-spark-k8s-image.sh

  # Or use remote JARs via S3:
  spark-submit \
      --master k8s://$API_SERVER \
      --deploy-mode cluster \
      --name lakehouse-pipeline \
      --conf spark.kubernetes.container.image=apache/spark:4.1.0-scala2.13-java21-python3-r-ubuntu \
      --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
      --conf spark.kubernetes.namespace=spark-jobs \
      --conf spark.executor.instances=3 \
      --conf spark.executor.memory=4g \
      --conf spark.executor.cores=2 \
      --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
      --conf spark.sql.catalog.iceberg.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
      --conf spark.sql.catalog.iceberg.uri=http://unity-catalog-service:8080/api/2.1/unity-catalog/iceberg \
      --conf spark.hadoop.fs.s3a.endpoint=http://seaweedfs-service:8333 \
      --conf spark.hadoop.fs.s3a.access.key=${S3_ACCESS_KEY} \
      --conf spark.hadoop.fs.s3a.secret.key=${S3_SECRET_KEY} \
      --conf spark.hadoop.fs.s3a.path.style.access=true \
      --jars local:///opt/spark/jars-extra/iceberg-spark-runtime-4.0_2.13-1.10.0.jar \
      local:///scripts/pipelines/pipeline_spark41.py

  # Key difference from standalone:
  #   - --master k8s:// instead of spark://
  #   - Services accessed via K8s DNS (unity-catalog-service, seaweedfs-service)
  #   - Secrets via K8s Secrets, not .env file
  #   - Executors are K8s pods, auto-cleaned after job
LAKEHOUSE
echo ""

# ─── Step 5: SDP on K8s ─────────────────────────────────────
echo "--- Step 5: SDP Pipeline on K8s ---"
echo ""
cat <<'SDP_K8S'
  # spark-pipelines wraps spark-submit, so K8s just works:
  spark-pipelines run \
      --spec /scripts/pipelines/spark-pipeline.yml \
      --master k8s://$API_SERVER \
      --deploy-mode cluster \
      --conf spark.kubernetes.container.image=apache/spark:4.1.0-scala2.13-java21-python3-r-ubuntu \
      --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark

  # Same pipeline spec. Same Python code. Different orchestrator.
SDP_K8S
echo ""

# ─── Step 6: Spark Connect on K8s ───────────────────────────
echo "--- Step 6: Spark Connect Server on K8s ---"
echo ""
cat <<'CONNECT_K8S'
  # Deploy Connect server as a K8s Deployment
  kubectl apply -f - <<EOF
  apiVersion: apps/v1
  kind: Deployment
  metadata:
    name: spark-connect-server
    namespace: spark-jobs
  spec:
    replicas: 1
    selector:
      matchLabels:
        app: spark-connect
    template:
      metadata:
        labels:
          app: spark-connect
      spec:
        serviceAccountName: spark
        containers:
        - name: spark-connect
          image: apache/spark:4.1.0-scala2.13-java21-python3-r-ubuntu
          command: ["/opt/spark/sbin/start-connect-server.sh", "--wait"]
          ports:
          - containerPort: 15002
            name: grpc
          env:
          - name: SPARK_MASTER
            value: "k8s://https://kubernetes.default.svc:443"
  ---
  apiVersion: v1
  kind: Service
  metadata:
    name: spark-connect
    namespace: spark-jobs
  spec:
    type: LoadBalancer
    ports:
    - port: 15002
      targetPort: 15002
    selector:
      app: spark-connect
  EOF

  # From your laptop:
  pip install pyspark-client
  python -c "
  from pyspark.sql import SparkSession
  spark = SparkSession.builder.remote('sc://k8s-lb:15002').getOrCreate()
  spark.sql('SHOW TABLES IN iceberg.bronze').show()
  "
CONNECT_K8S
echo ""

# ─── Summary ────────────────────────────────────────────────
echo "--- Summary: Standalone vs K8s ---"
echo ""
cat <<'SUMMARY'
  ┌──────────────┬─────────────────────────────────┬─────────────────────────────────┐
  │ Aspect       │ Standalone (docker-compose)      │ Kubernetes                      │
  ├──────────────┼─────────────────────────────────┼─────────────────────────────────┤
  │ Master URL   │ spark://localhost:7078           │ k8s://https://api-server:6443   │
  │ Workers      │ Static in compose file           │ Dynamic pods, autoscaling       │
  │ Config       │ Volume mounts (spark-defaults)   │ ConfigMaps or baked in image    │
  │ JARs         │ Volume mounts (./jars)           │ Baked in image or remote S3     │
  │ Secrets      │ .env file                        │ K8s Secrets                     │
  │ Monitoring   │ localhost:8082                   │ kubectl port-forward            │
  │ Scaling      │ Manual                           │ Dynamic allocation              │
  │ Code changes │ NONE                             │ NONE                            │
  └──────────────┴─────────────────────────────────┴─────────────────────────────────┘

  Same image. Same code. Same pipeline. Different orchestrator.
SUMMARY
echo ""
echo "============================================================"
echo "  Act 5c complete."
echo "============================================================"
