---
name: DE Troubleshooter & Debug
description: Systematic troubleshooting and diagnostic playbook for local Docker containers, Kafka partition lag, MinIO S3 connectivity, PySpark Java Gateway errors, Delta log locks, and Airflow worker failures.
---

# DE Troubleshooter & Debug

The **DE Troubleshooter & Debug** skill equips agents and engineers with rapid, systematic triage playbooks when running the local Docker Lakehouse stack.

---

## 1. Quick Diagnostic Triage Table

| Symptom | Probable Cause | Fast Diagnostic Command | Recommended Remediation |
| :--- | :--- | :--- | :--- |
| **Kafka Connection Refused** (`9092`) | Broker starting or listener misconfigured | `docker logs dp-kafka --tail 50` | Verify `INTERNAL://kafka:29092` vs `EXTERNAL://localhost:9092`. Wait for healthcheck. |
| **MinIO Connection Timeout** (`9000`) | S3 endpoint unreachable or bucket missing | `curl -I http://localhost:9000/minio/health/live` | Verify `mc ls local/` and check S3 endpoint URL configuration (`http://minio:9000`). |
| **Delta Lake Concurrent Write Error** | Competing jobs writing same table partition | Check `_delta_log/` for concurrent commits | Implement pre-aggregated micro-batch merge and avoid overlapping writer queries. |
| **PySpark Java Gateway Error** | Java version mismatch or JVM out of memory | `java -version` & check `JAVA_HOME` | Verify Java 11 or 17. Ensure `spark.driver.memory=2g`. |
| **dbt-spark Thrift Connection Refused** | Spark Thrift Server or Spark Connect container not ready | `docker compose ps` | Check container status. Re-run after service reports healthy. |

---

## 2. Standard Diagnostic Procedure

1. **Check Service Topology:**
   ```bash
   docker compose ps
   ```
2. **Inspect Specific Container Logs:**
   ```bash
   docker compose logs kafka --tail 100
   docker compose logs minio --tail 100
   ```
3. **Verify Kafka Broker Topic Partitions:**
   ```bash
   docker exec -it dp-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic payment.events.raw
   ```
4. **Verify MinIO S3 Buckets:**
   ```bash
   docker exec -it dp-minio mc alias list
   ```
