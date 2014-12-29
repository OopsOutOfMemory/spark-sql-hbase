#Spark SQL HBase Connector
 
 
 _Spark SQL HBase Connector_ aim to query HBase Table by using Spark SQL.
 
 It leverages the functionality of [Spark SQL](http://spark.apache.org/sql/) 1.2+ external datasource API .

> 作为一个中国人，我还是写几句中文 ：）
Spark1.2发布之后，Spark SQL支持了External Datasource API，我们才能方便的编写扩展来使Spark SQL能支持更多的外部数据源。
鉴于国内用HBase的用户比较多，所以使Spark SQL支持查询HBase还是很有价值的，这也是我写这个Lib的原因。
不过，一个人得力量远不如大家的力量，所以希望大家能多提Issues，多多Commit~ 先谢谢了：）


##Using SQL Resiger HBase Table

###1.Query by Spark SQL

#### One Column and Multiple Columns Scan.

Recommended way is to always put the rowkey at the first column in schema. 
And we use `:key` represent the rowkey in hbase.   

`sparksql_table_schema`: is the table will register to spark sql. <br/>
`hbase_table_name`: a real hbase table name in hbase. <br/>
`hbase_table_schema`: the columns want's to query in hbase table __hbase_table_name__ you provided.  <br/>
 
 __Note__:
`sparksql_table_schema` and `hbase_table_schema` should be a mapping relation, should have same column number and index.

  ```scala
  import org.apache.spark.sql.SQLContext  
  val sqlContext  = new SQLContext(sc)
  import sqlContext._
  
  val hbaseDDL = s"""
        |CREATE TEMPORARY TABLE hbase_people
        |USING com.shengli.spark.hbase
        |OPTIONS (
        |  sparksql_table_schema   '(row_key string, name string, age int, job string)',
        |  hbase_table_name    'people',
        |  hbase_table_schema '(:key , profile:name , profile:age , career:job )'
        |)""".stripMargin
  
        
  sqlContext.sql(hbaseDDL)
  sql("select row_key,name,age,job from hbase_people").collect()
```

Let's see the result:

__select__:

```
scala> sql("select row_key,name,age,job from hbase_people").collect()
14/12/27 02:24:22 INFO scheduler.DAGScheduler: Job 0 finished: collect at SparkPlan.scala:81, took 1.576415 s
res1: Array[org.apache.spark.sql.Row] = Array([rowkey001,Sheng,Li,25,software engineer], [rowkey002,Li,Lei,26,teacher], [rowkey003,Jim Green,24,english teacher], [rowkey004,Lucy,23,doctor], [rowkey005,HanMeiMei,18,student])
```

__functions__:

__avg__:

```scala
scala> sql("select avg(age) from hbase_people").collect()
14/12/27 02:26:55 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 2.0, whose tasks have all completed, from pool 
14/12/27 02:26:55 INFO scheduler.DAGScheduler: Job 1 finished: collect at SparkPlan.scala:81, took 0.459760 s
res2: Array[org.apache.spark.sql.Row] = Array([23.2])
```
__count:__

```scala
scala> sql("select count(1) from hbase_people").collect()
res3: Array[org.apache.spark.sql.Row] = Array([5])
```

### Support RowKey Range Scan

If you need a range data from a hbase table, you can specify `row_range` in __OPTIONS__.
We only need start rowkey is `rowkey003` and end rowkey is `rowkey005`

```
 val hbaseDDL = s"""
       |CREATE TEMPORARY TABLE hbase_people
       |USING com.shengli.spark.hbase
       |OPTIONS (
       |  sparksql_table_schema   '(row_key string, name string, age int, job string)',
       |  hbase_table_name    'people',
       |  hbase_table_schema '(:key , profile:name , profile:age , career:job )',
       |  row_range  'rowkey003->rowkey005'
       |)""".stripMargin
```

By using RowKey Range Scan, the result of the query only return:
```
res2: Array[org.apache.spark.sql.Row] = Array([rowkey003,Jim Green,24,english teacher], [rowkey004,Lucy,23,doctor])
```

And the count is:
```
scala> sql("select count(1) from hbase_people").collect()
res3: Array[org.apache.spark.sql.Row] = Array([2])
```


###2. Query by SQLContext API

Firstly, import `import com.shengli.spark.hbase._`
Secondly, use `sqlContext.hbaseTable` _API_ to generate a `SchemaRDD`
The `sqlContext.hbaseTable` _API_ need serveral parameters.

__Common Way__:

If you do common Scan, you just pass three parameters below:

```scala
   sqlContext.hbaseTable(sparksqlTableSchema: String, hbaseTableName: String, hbaseTableSchema: String) 
```

```scala
scala> import com.shengli.spark.hbase._
import com.shengli.spark.hbase._

scala> val hbaseSchema = sqlContext.hbaseTable("(row_key string, name string, age int, job string)","people","(:key string, profile:name , profile:age , career:job )")
......
14/12/27 02:30:55 INFO spark.SparkContext: Created broadcast 4 from newAPIHadoopRDD at HBaseRelation.scala:158
hbaseSchema: org.apache.spark.sql.SchemaRDD = 
SchemaRDD[16] at RDD at SchemaRDD.scala:108
== Query Plan ==
== Physical Plan ==
PhysicalRDD [row_key#15,name#16,age#17,job#18], MapPartitionsRDD[19] at map at HBaseRelation.scala:166
```

We've got a hbaseSchema so that we can query it with DSL or register it as a temp table query with sql, do whatever you like:
```
scala> hbaseSchema.select('row_key).collect()
res1: Array[org.apache.spark.sql.Row] = Array([rowkey001], [rowkey002], [rowkey003], [rowkey004], [rowkey005])
```

__RowKey Range Scan__:

RowKey Range Scan need pass a `row_range` which format is `starRow->endRow` to let the connector know:

```scala
sqlContext.hbaseTable(sparksqlTableSchema: String, hbaseTableName: String, hbaseTableSchema: String, rowRange: String)
```


```scala
scala> import com.shengli.spark.hbase._
import com.shengli.spark.hbase._

scala> val hbaseSchema = sqlContext.hbaseTable("(row_key string, name string, age int, job string)","people","(:key string, profile:name , profile:age , career:job )","rowkey002->rowkey004")
hbaseSchema: org.apache.spark.sql.SchemaRDD = 
SchemaRDD[9] at RDD at SchemaRDD.scala:108
== Query Plan ==
== Physical Plan ==
PhysicalRDD [row_key#8,name#9,age#10,job#11], MapPartitionsRDD[12] at map at HBaseRelation.scala:174

scala> hbaseSchema.select('row_key).collect()
......
res0: Array[org.apache.spark.sql.Row] = Array([rowkey002], [rowkey003])
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

###Note:

####Package

In the root directory,  use `sbt package` to package the lib.

####Dependency

__1. hbase-site.xml__

You need place `hbase-site.xml` under the spark classpath. Also need to configure it correctly first.
Below is my hbase-site.xml:

```scala
<configuration>
 <property>
     <name>hbase.rootdir</name>
     <value>file:///Users/shengli/software/data/hbase</value>
 </property>
 <property>
     <name>hbase.cluster.distributed</name>
         <value>true</value>
 </property>
 <property>
      <name>hbase.zookeeper.property.clientPort</name>
               <value>2181</value>
  </property>

 <property>
     <name>hbase.zookeeper.quorum</name>
     <value>localhost</value>
  </property>
  <property>
      <name>hbase.defaults.for.version.skip</name>
          <value>true</value>
  </property>
</configuration>
```

You can simply do it with `ln -s ~/software/hbase/conf/hbase-site.xml ~/git_repos/spark`

__2. Add hbase related libs into spark classpath__

Below is how I start the spark shell:<br/>
Add hbase related libs into spark classpath to make sure spark can access hbase with spark api first.
<br/>
Then:

```scala
bin/spark-shell --master spark://192.168.2.100:7077 --jars /Users/shengli/software/hbase/lib/hbase-client-0.98.8-hadoop2.jar,/Users/shengli/software/hbase/lib/hbase-server-0.98.8-hadoop2.jar,/Users/shengli/software/hbase/lib/hbase-common-0.98.8-hadoop2.jar,/Users/shengli/software/hbase/lib/hbase-protocol-0.98.8-hadoop2.jar,/Users/shengli/software/hbase/lib/protobuf-java-2.5.0.jar,/Users/shengli/software/hbase/lib/htrace-core-2.04.jar,/Users/shengli/git_repos/spark-sql-hbase/target/scala-2.10/spark-sql-hbase_2.10-0.1.jar --driver-java-options "-Dsun.io.serialization.extendedDebugInfo=true"
```

__3. class not found issues__

The below provides the mapping of the classes and their respective jars

```scala
| Class Name |	Jar Name       |
|------------|-----------------|
| TableSplit |	hbase-server.jar |
| HTable |	hbase-client.jar |
| MasterProtos |	hbase-protocol.jar |
| org.cloudera.htrace.Trace |	htrace-core-2.01.jar |
```

- https://support.pivotal.io/hc/en-us/articles/203025186-Hive-Query-from-Tableau-failed-with-error-Execution-Error-return-code-2-from-org-apache-hadoop-hive-ql-exec-mr-MapRedTask

###Contact Me
如果有任何疑问，可以通过以下方式联系我：
- WeiBo: http://weibo.com/oopsoom
- Blog: http://blog.csdn.net/oopsoom