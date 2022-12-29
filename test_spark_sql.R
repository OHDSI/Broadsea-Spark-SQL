library(DatabaseConnector)

# call this function one time to download the Spark jdbc driver
# DatabaseConnector::downloadJdbcDrivers(dbms='spark', pathToDriver='~/jdbcdrivers')

# connect to Spark thrift server running inside Broadsea-Spark-SQL Docker container
conn <- connect(dbms='spark',
                user = 'secret-userid',
                password = 'secret-password',
                connectionString='jdbc:spark://0.0.0.0:10000;UseNativeQuery=1')

dbGetQuery(conn, 'show databases;')
dbGetQuery(conn, 'show tables;')

data <- data.frame(x = c(1, 2, 3), y = c("a", "b", "c"))

# use try() to handle the error message from Spark Simba JDBC driver when calling insertTable():
# java.sql.SQLException: [Simba][JDBC](10040) Cannot use commit while Connection is in auto-commit mode.
try(insertTable(conn, "default", "my_table", data))
dbGetQuery(conn, 'show tables;')

dbGetQuery(conn, 'select * from my_table;')
# SQL query output:
# x y
# 2 b
# 3 c
# 1 a

executeSql(conn, 'delete from my_table where x = 3;')
dbGetQuery(conn, 'select * from my_table;')
# SQL query output:
# x y
# 1 a
# 2 b

executeSql(conn, 'drop table if exists my_table;')
dbGetQuery(conn, 'show tables;')

disconnect(conn)
