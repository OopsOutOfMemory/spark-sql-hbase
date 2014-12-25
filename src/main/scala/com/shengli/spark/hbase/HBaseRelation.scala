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
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.sources.TableScan
import scala.collection.immutable.{HashMap, Map}
import org.apache.hadoop.hbase.client.{Scan, HTable}

import org.apache.spark.sql._
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.apache.hadoop.hbase.client.HBaseAdmin


/**
 *  CREATE TEMPORARY TABLE hbaseTable
      USING com.shengli.spark.hbase
      OPTIONS (
        registerTableSchema   '(rowkey string, value string)',
        externalTableName    'test',
        externalTableSchema '(rowkey:rowkey string, f1:col1 string)'
      )
 */

case class HBaseRelation(hbaseProps: Map[String,String])(@transient val sqlContext: SQLContext) extends TableScan {
  val externalTableName =  hbaseProps.getOrElse("externalTableName", sys.error("not valid schema"))
  val externalTableSchema =  hbaseProps.getOrElse("externalTableSchema", sys.error("not valid schema"))

  val registerTableSchema = hbaseProps.getOrElse("registerTableSchema", sys.error("not valid schema"))

  val tableName = hbaseProps.getOrElse("tableName",sys.error("no table name found!"))
  lazy val schema = {
    //should return StructType
    val externalTableFields = extractExternalSchema(externalTableSchema)
    val fields = externalTableFields.map{
      case  field : HBaseSchemaField =>
        val name  = field.fieldName.replace(":","_")
        val relatedType =  field.fieldType match  {
          case "string" =>
            SchemaType(StringType,nullable = false)
          case "int" =>
            SchemaType(IntegerType,nullable = false)
          case "long" =>
            SchemaType(LongType,nullable = false)
        }
        StructField(name,relatedType.dataType,relatedType.nullable)
    }
    StructType(fields)
  }

    /**
     * spark sql schema will be register
     *   registerTableSchema   '(rowkey string, value string)'
      */
  def extractRegisterSchema(registerTableSchema: String) : Array[RegisteredSchemaField] = {
         val fieldsStr = registerTableSchema.trim.drop(1).dropRight(1)
         val fieldsArray = fieldsStr.split(",").map(_.trim)
         fieldsArray.map{ fildString =>
           val splitedField = fildString.split("\\s+", -1)
           RegisteredSchemaField(splitedField(0), splitedField(1))
         }
   }

  //externalTableSchema '(:key, f1:col1)'
  def extractExternalSchema(externalTableSchema: String) : Array[HBaseSchemaField] = {
        val fieldsStr = externalTableSchema.trim.drop(1).dropRight(1)
        val fieldsArray = fieldsStr.split(",").map(_.trim)
        fieldsArray.map{ fildString =>
          val splitedField = fildString.split(":", -1)
          HBaseSchemaField(splitedField(0), splitedField(1))
    }
  }


  // By making this a lazy val we keep the RDD around, amortizing the cost of locating splits.
  lazy val buildScan = {

    val hbaseTableFields = extractExternalSchema(externalTableSchema)
    val hbaseConf = HBaseConfiguration.create()
    //This should be kv pairs
    var rowKey: String = null
    var columnFamilyName: String  = null
    var columnName: String  = null

    hbaseTableFields.foreach{field=>
      val cf = field.fieldName.split(":",-1)(0)
      val col = field.fieldName.split(":",-1)(1)
      if(cf == col && cf=="rowkey") {
        rowKey = "rowkey"
      }
      else{
        //currently we only support to query one column of one cf
        columnFamilyName = cf
        columnName = col
      }
    }

    hbaseConf.set(TableInputFormat.INPUT_TABLE, externalTableName)
    hbaseConf.set(TableInputFormat.SCAN_COLUMNS, columnFamilyName+":"+columnName);

    val hbaseRdd = sqlContext.sparkContext.newAPIHadoopRDD(
      hbaseConf,
      classOf[org.apache.hadoop.hbase.mapreduce.TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )

    //should be a method to generate different rdd

    //for string
    //Array[(String,String)]   rowkey, cf column value
    val rs = hbaseRdd.map(tuple => tuple._2).map(result => {
      val values = List(result.getRow.map(_.toChar).mkString, result.value.map(_.toChar).mkString)
      Row.fromSeq(values.toSeq)
    })
    rs
  }

  private case class SchemaType(dataType: DataType, nullable: Boolean)
//
//  private def toSqlType(hbaseSchema: Schema): SchemaType = {
//    SchemaType(StringType,true)
//  }
}