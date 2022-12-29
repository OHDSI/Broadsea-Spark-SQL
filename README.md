# Broadsea-Spark-SQL

Spark SQL with Delta Lake extension - a Docker container for OHDSI software testing
based on the bitnami/spark Docker image.

Run the Docker container
```bash
docker-compose up -d
```

Remove the Docker container - *ALL THE DATA IN SPARK SQL WILL BE LOST*
```bash
docker-compose down
```

See the file "test_spark_sql.R" in this GitHub repository for an example of accessing 
this Spark SQL Docker container using the OHDSI HADES DatabaseConnector R package.
Any values may be used for the userid and password.

Optionally, connect to Spark SQL using a SQL client application and execute the SQL file "load_eunomia_demo_cdm_data.sql" in 
this GitHub repository. It will create and populate a Eunomia OMOP CDM by loadibg a tiny Eunomia synthetic data set 
(gzipped csv files) stored within the Docker container.

Example JDBC connection string. It is important to include 'UseNativeQuery=1' if using the Simba JDBC driver:
```
jdbc:spark://0.0.0.0:10000;UseNativeQuery=1
```
 
## Other useful Docker commands

Pull a specific Docker image version from Docker Hub
```bash
docker pull ohdsi/broadsea-spark-sql:1.0.2
```

Build the Docker image - in general this is not needed, use the image in Docker Hub instead
```bash
docker-compose build
```
