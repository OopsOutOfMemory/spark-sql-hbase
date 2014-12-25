#Spark SQL HBase Connector
 
 
 _Spark SQL HBase Connector_ aim to query HBase Table by using Spark SQL.
 
 It leverages the functionality of [Spark SQL](http://spark.apache.org/sql/) 1.2+ external datasource API .
 >作为一个中国人，我还是写几句中文 ：）
Spark1.2发布之后，Spark SQL支持了External Datasource API，我们才能方便的编写扩展来使Spark SQL能支持更多的外部数据源。
鉴于国内用HBase的用户比较多，所以使Spark SQL支持查询HBase还是很有价值的，这也是我写这个Lib的原因。
不过，一个人得力量远不如大家的力量，所以希望大家能多提Issues，多多Commit~ 先谢谢了：）

####Concepts

__1、External Table__

For HBase Table:
We call it `external table` because it's outside of spark sql.

__2、Register Table__

As We know for one specific `HBase Table` there should be a `Spark SQL Table` which is a mapping of it in Spark SQL. We call it `registerTable`.


####Using SQL Resiger HBase Table

####1. Query by Spark SQL
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
	sql("select row_key,name from hbase_people").collect()
```
Let's see the result:
```
scala> sql("select row_key,name from hbase_people").collect()
14/12/26 00:42:13 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 3.0, whose tasks have all completed, from pool 
14/12/26 00:42:13 INFO scheduler.DAGScheduler: Job 3 finished: collect at SparkPlan.scala:81, took 0.140132 s
res11: Array[org.apache.spark.sql.Row] = Array([rowkey001,Sheng,Li], [rowkey002,Li,Lei], [rowkey003,Jim Green], [rowkey004,Lucy], [rowkey005,HanMeiMei])
```

####2. Query by SQLContext API

Firstly, import `import com.shengli.spark.hbase._`
Secondly, use `sqlContext.hbaseTable` _API_ to generate a `SchemaRDD`
The `sqlContext.hbaseTable` _API_ need serveral parameters.

```scala
hbaseTable(registerTableSchema: String, externalTableName: String, externalTableSchema: String)__
```

Let me give you a detail example:

```scala
scala> import com.shengli.spark.hbase._
import com.shengli.spark.hbase._

scala> val hbaseSchema = sqlContext.hbaseTable("(row_key string, name string)","people","(rowkey:rowkey string, profile:name string)")
......
14/12/26 00:13:09 INFO spark.SparkContext: Created broadcast 3 from newAPIHadoopRDD at HBaseRelation.scala:113
hbaseSchema: org.apache.spark.sql.SchemaRDD = 
SchemaRDD[13] at RDD at SchemaRDD.scala:108
== Query Plan ==
== Physical Plan ==
PhysicalRDD [row_key#4,name#5], MapPartitionsRDD[16] at map at HBaseRelation.scala:121
```

We've got a hbaseSchema so that we can query it with DSL or register it as a temp table query with sql, do whatever you like:

```scala
scala> hbaseSchema.select('name).collect()
14/12/26 00:14:30 INFO util.RegionSizeCalculator: Calculating region sizes for table "people".
14/12/26 00:14:30 INFO spark.SparkContext: Starting job: collect at SparkPlan.scala:81
14/12/26 00:14:30 INFO scheduler.DAGScheduler: Got job 1 (collect at SparkPlan.scala:81) with 1 output partitions (allowLocal=false)
14/12/26 00:14:30 INFO scheduler.DAGScheduler: Final stage: Stage 1(collect at SparkPlan.scala:81)
14/12/26 00:14:30 INFO scheduler.DAGScheduler: Parents of final stage: List()
......
14/12/26 00:14:30 INFO scheduler.DAGScheduler: Job 1 finished: collect at SparkPlan.scala:81, took 0.205903 s
res9: Array[org.apache.spark.sql.Row] = Array([Sheng,Li], [Li,Lei], [Jim Green], [Lucy], [HanMeiMei])
```

##HBase Data

Let's take look at the `HBase Table` named `person`

The `schema` of the table `person`:

__column family__: `profile`, `career`

__coloumns__:`profile:name`, `profile:age`,`carrer:job`


```java
1.8.7-p357 :024 > scan 'people'
ROW                                  COLUMN+CELL                                                                                               
 rowkey001                           column=career:job, timestamp=1419517844784, value=software engineer                                       
 rowkey001                           column=profile:age, timestamp=1419517844665, value=25                                                     
 rowkey001                           column=profile:name, timestamp=1419517844501, value=Sheng,Li                                              
 rowkey002                           column=career:job, timestamp=1419517844813, value=teacher                                                 
 rowkey002                           column=profile:age, timestamp=1419517844687, value=26                                                     
 rowkey002                           column=profile:name, timestamp=1419517844544, value=Li,Lei                                                
 rowkey003                           column=career:job, timestamp=1419517844832, value=english teacher                                         
 rowkey003                           column=profile:age, timestamp=1419517844704, value=24                                                     
 rowkey003                           column=profile:name, timestamp=1419517844568, value=Jim Green                                             
 rowkey004                           column=career:job, timestamp=1419517844853, value=doctor                                                  
 rowkey004                           column=profile:age, timestamp=1419517844724, value=23                                                     
 rowkey004                           column=profile:name, timestamp=1419517844589, value=Lucy                                                  
 rowkey005                           column=career:job, timestamp=1419517845664, value=student                                                 
 rowkey005                           column=profile:age, timestamp=1419517844744, value=18                                                     
 rowkey005                           column=profile:name, timestamp=1419517844606, value=HanMeiMei                                             
5 row(s) in 0.0260 seconds
```


###Contact Me
如果有任何疑问，可以通过以下方式联系我：
- WeiBo: http://weibo.com/oopsoom
- Blog: http://blog.csdn.net/oopsoom
- 邮箱：<victorshengli@126.com> 常用
- 邮箱：<victorsheng117@gmail.com> 需要VPN，不常使用