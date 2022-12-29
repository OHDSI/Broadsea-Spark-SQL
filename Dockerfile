FROM bitnami/spark

MAINTAINER Lee Evans - www.ltscomputingllc.com

USER root

# install maven
RUN apt-get update && apt-get -y install maven

# add delta lake support
RUN mvn dependency:copy -Dartifact=io.delta:delta-core_2.12:2.1.0 -DoutputDirectory=/spark_home/jars
RUN mvn dependency:copy -Dartifact=io.delta:delta-storage:2.1.0 -DoutputDirectory=/spark_home/jars

# read in Eunomia demo CDM csv gzip compressed files and decompress them
RUN mkdir /demo_cdm_csv_files
COPY demo_cdm_csv_files/ /demo_cdm_csv_files/
WORKDIR /demo_cdm_csv_files
RUN gunzip *.gz

# needed so that thrift server can create the megastore_db directory
RUN chmod g+w /opt/bitnami/spark
RUN chmod g+w /spark_home
WORKDIR /spark_home

USER 1001
CMD ["/bin/bash", "-c","/opt/bitnami/spark/bin/spark-submit --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 --packages 'io.delta:delta-core_2.12:2.1.0' --conf 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog'"]
