#!/bin/sh

app_name=$1
queries=$2

array=(${queries//,/ })
for query in ${array[@]}
do
   query=$query"-v2.4"

spark-submit \
--master k8s://https://192.168.45.93:5443 \
--deploy-mode cluster \
--name ${app_name}${query} \
--class com.databricks.spark.sql.perf.BenchmarkQuery \
--conf spark.kubernetes.volcano.enable=true \
--conf spark.kubernetes.volcano.podgroup.cpu=3 \
--conf spark.kubernetes.volcano.podgroup.memory=12g \
--conf spark.driver.memory=2G \
--conf spark.executor.memory=2G \
--conf spark.kubernetes.executor.request.cores=500m \
--conf spark.driver.cores=500m \
--conf spark.executor.instances=5 \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
--conf spark.kubernetes.container.image.pullSecrets=default-secret \
--conf spark.scheduler.minRegisteredResourcesRatio=0.1 \
--conf spark.kubernetes.memoryOverheadFactor=0.2 \
--conf spark.kubernetes.submission.waitAppCompletion=false \
--conf spark.kubernetes.container.image=<image> \
local:///opt/spark/jars/spark-sql-perf_2.11-0.5.1-SNAPSHOT.jar ${app_name}${query} obs://volcano-wlb/spark/scale-factor-350/ "350" obs://volcano-wlb/spark/scale-factor-350-result/ ${query} &

done
