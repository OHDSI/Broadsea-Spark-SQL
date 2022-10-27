library(DatabaseConnector)

# call this function one time to download the Spark jdbc driver
DatabaseConnector::downloadJdbcDrivers(dbms='spark', pathToDriver='~/jdbcdrivers') 

# connect to Spark thrift server running inside Broadsea-Spark-SQL Docker container
conn <- connect(dbms='spark',
                user = 'secret-userid',
                password = 'secret-password',
                connectionString='jdbc:spark://0.0.0.0:10000;')

dbGetQuery(conn, 'show databases;')

executeSql(conn, 'drop table if exists my_table;')

data <- data.frame(x = c(1, 2, 3), y = c("a", "b", "c"))
insertTable(conn, "default", "my_table", data)

dbGetQuery(conn, 'select * from my_table;')
# SQL query output:
# x y
# 1 2 b
# 2 3 c
# 3 1 a

disconnect(conn)
