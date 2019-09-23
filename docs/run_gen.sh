#!/bin/sh

   spark-submit \
--master k8s://https://192.168.2.233:5443 \
--deploy-mode cluster \
--name "generate" \
--class com.databricks.spark.sql.perf.BenchmarkGen \
--conf spark.driver.memory=2G \
--conf spark.executor.memory=8G \
--conf spark.kubernetes.executor.request.cores=1 \
--conf spark.executor.instances=5 \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
--conf spark.kubernetes.container.image.pullSecrets=default-secret \
--conf spark.scheduler.minRegisteredResourcesRatio=0.1 \
--conf spark.kubernetes.memoryOverheadFactor=0.2 \
--conf spark.kubernetes.submission.waitAppCompletion=false \
--conf spark.kubernetes.container.image=<image location> \
local:///opt/spark/jars/spark-sql-perf_2.11-0.5.1-SNAPSHOT.jar "generate" /tmp/test "10" & 