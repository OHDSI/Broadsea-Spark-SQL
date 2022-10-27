# Broadsea-Spark-SQL

Spark SQL with Delta Lake extension - a Docker container for OHDSI software testing

Run the Docker container
```bash
docker-compose up -d
```

Stop the Docker container - **ALL THE DATA IN SPARK SQL WILL BE LOST*
```bash
docker-compose down
```

See the file "test_spark_sql.R" in this GitHub repository for an example of accessing 
this Spark SQL Docker container using the OHDSI HADES DatabaseConnector R package.

## Other useful Docker commands

Pull a specific Docker image version from Docker Hub
```bash
docker pull ohdsi/broadsea-spark-sql:1.0.0
```

Build the Docker image - in general this is not needed, use the image in Docker Hub instead
```bash
docker-compose build
```
