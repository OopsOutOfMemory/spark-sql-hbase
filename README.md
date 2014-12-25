##Spark HBase External DataSource

###Spark SQL access HBase Table

###Example:

```scala
 import org.apache.spark.sql.SQLContext
   val sqlContext  = new SQLContext(sc)
   import sqlContext._

   val s = s"""
      |CREATE TEMPORARY TABLE hbaseTable
      |USING com.shengli.spark.hbase
      |OPTIONS (
      |  registerTableSchema   '(row_key string, column_a string)',
      |  externalTableName    'test',
      |  externalTableSchema '(rowkey:rowkey string, cf:a string)'
      |)""".stripMargin

	sqlContext.sql(s).collect()
	sql("select column_a from hbaseTable").collect()
```