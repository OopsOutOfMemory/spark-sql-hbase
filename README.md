 #Spark SQL HBase Connector
 
 Spark SQL HBase connector aim to query hbase table by using spark sql.
 It leverages the functionality of [Spark SQL](http://spark.apache.org/sql/) 1.2+ external datasource API .
 
 ####Using SQL Resiger HBase Table
  ```scala
   import org.apache.spark.sql.SQLContext  
   import sqlContext._

   val sqlContext  = new SQLContext(sc)  
   val hbaseDDL = s"""
      |CREATE TEMPORARY TABLE hbase_people
      |USING com.shengli.spark.hbase
      |OPTIONS (
      |  registerTableSchema   '(row_key string, name string)',
      |  externalTableName    'people',
      |  externalTableSchema '(rowkey:rowkey string, profile:name string)'
      |)""".stripMargin
	  
	sqlContext.sql(hbaseDDL)
	sql("select name from hbase_people").collect()
```
