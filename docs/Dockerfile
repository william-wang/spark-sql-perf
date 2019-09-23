FROM volcano/spark:0.1.0-obs

ADD spark-sql-perf_2.11-0.5.1-SNAPSHOT.jar /opt/spark/jars/
ADD spark-kubernetes_2.11-2.4.4-SNAPSHOT.jar /opt/spark/jars/
ADD tools /opt/tpcds-kit/tools/
ADD ld-linux-x86-64.so.2 /usr/lib/