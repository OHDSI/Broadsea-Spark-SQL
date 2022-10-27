FROM bitnami/spark
USER root

# install maven
RUN apt-get update && apt-get -y install maven

# add delta lake support
WORKDIR /spark_home
RUN mvn dependency:copy -Dartifact=io.delta:delta-core_2.12:2.1.0 -DoutputDirectory=/spark_home/jars
RUN mvn dependency:copy -Dartifact=io.delta:delta-storage:2.1.0 -DoutputDirectory=/spark_home/jars

# needed so that thrift server can create the megastore_db directory
RUN chmod g+w /opt/bitnami/spark
RUN chmod g+w /spark_home/

USER 1001
CMD ["/bin/bash", "-c","/opt/bitnami/spark/bin/spark-submit --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 --packages 'io.delta:delta-core_2.12:2.1.0' --conf 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog'"]