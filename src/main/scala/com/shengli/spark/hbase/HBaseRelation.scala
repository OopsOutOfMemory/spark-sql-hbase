/*
* Copyright 2014 Sheng, Li
*
* Licensed under the Apache License, Version 2.0 (the "License");
*mapu Map[String, String]le except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.shengli.spark.hbase
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.sources.TableScan
import scala.collection.immutable.Map
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Scan, HTable}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat


case class HBaseRelation(hbaseProps: Map[String,String])(@transient val sqlContext: SQLContext) extends TableScan {

  val schema = {
    //should return StructType
    //TODO
  }
  // we need to check the required parameters first
    /**
     * tableName
     * hbase.columns.mapping
     * zookeeperAddress  localhost:2181
    */

  private def checkRequireedHbaseConf(hbaseProps: Map[String,String]) {

  }
  // By making this a lazy val we keep the RDD around, amortizing the cost of locating splits.
  lazy val buildScan = {

    val hbaseConf = HBaseConfiguration.create();
    val tableName = hbaseProps.getOrElse("tableName",sys.error("no table name found!"))
    val zookeeper = hbaseProps.getOrElse("zookeeperAddress",sys.error("no zookeeper address found!"))
    //This should be kv pairs
    val columnFamilyName = hbaseProps.getOrElse("cfs",sys.error("no cfs found!"))
    val columnName = hbaseProps.getOrElse("columns",sys.error("no columns found!"))


    hbaseConf.set("hbase.zookeeper.property.clientPort", "2223")
    hbaseConf.set("hbase.zookeeper.quorum", "localhost");
    hbaseConf.set("hbase.master", "localhost:60000");
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)


    val hbaseRdd = sqlContext.sparkContext.newAPIHadoopRDD(
      hbaseConf,
      classOf[org.apache.hadoop.hbase.mapreduce.TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )

    //should be a method to generate different rdd
    hbaseRdd.map(tuple => tuple._2).map(result => (result.getRow, result.getColumn("f1".getBytes(), "col1".getBytes()))).map(row => {
      (
        row._1.map(_.toChar).mkString,
        row._2.asScala.reduceLeft {
          (a, b) => if (a.getTimestamp > b.getTimestamp) a else b
        }.getValue.map(_.toChar).mkString
        )
    })
  }

  private case class SchemaType(dataType: DataType, nullable: Boolean)

//  private def toSqlType(hbaseSchema: Schema): SchemaType = {
//    SchemaType(StringType,true)
//  }
}