/*
* Copyright 2014 Sheng, Li
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
        registerTableSchema   '(rowkey string, f1col1 string)',
        externalTableName    'test',
        externalTableSchema '(rowkey:rowkey string, f1:col1 string)'
      )
 */
case class HBaseRelation(hbaseProps: Map[String,String])(@transient val sqlContext: SQLContext) extends TableScan {
  val externalTableName =  hbaseProps.getOrElse("externalTableName", sys.error("not valid schema"))
  val externalTableSchema =  hbaseProps.getOrElse("externalTableSchema", sys.error("not valid schema"))
  val registerTableSchema = hbaseProps.getOrElse("registerTableSchema", sys.error("not valid schema"))

  val externalTableFields = extractExternalSchema(externalTableSchema)
  val registerTableFields = extractRegisterSchema(registerTableSchema)

  lazy val schema = {
    val fieldsRelations = fieldsMapping(externalTableFields,registerTableFields)
    val fields = externalTableFields.map{ field=>
        val name  = fieldsRelations.getOrElse(field.fieldName, sys.error("table schema is not match the definition."))
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

  def fieldsMapping( externalHBaseTable: Array[HBaseSchemaField],  registerTable : Array[RegisteredSchemaField]): Map[String, String] = {
       if(externalHBaseTable.length != registerTable.length) sys.error("columns size not match in definition!")
       val length =  externalHBaseTable.length
       //Array[(externalHBaseTable, registerTable)]
       val names = externalHBaseTable.map(_.fieldName)
       val targetNames =  registerTable.map(_.fieldName)
       val rs = names.zip(targetNames)
       rs.toMap
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

  //externalTableSchema '(rowkey:rowkey string, f1:col1 string)'
  def extractExternalSchema(externalTableSchema: String) : Array[HBaseSchemaField] = {
        val fieldsStr = externalTableSchema.trim.drop(1).dropRight(1)
        val fieldsArray = fieldsStr.split(",").map(_.trim)
        fieldsArray.map{ fildString =>
          val fieldsNameTypeArray = fildString.trim.split("\\s+",-1)
          HBaseSchemaField(fieldsNameTypeArray(0), fieldsNameTypeArray(1))
    }
  }


  // By making this a lazy val we keep the RDD around, amortizing the cost of locating splits.
  lazy val buildScan = {

    val hbaseConf = HBaseConfiguration.create()
    //This should be kv pairs
    var rowKey: String = null
    var columnFamilyName: String  = null
    var columnName: String  = null

    externalTableFields.foreach{field=>
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