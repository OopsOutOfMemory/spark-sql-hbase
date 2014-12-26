/*
* Copyright 2014 Sheng, Li
*/
package com.shengli.spark.hbase
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.sources.TableScan
import scala.collection.immutable.{HashMap, Map}
import org.apache.hadoop.hbase.client.{Result, Scan, HTable, HBaseAdmin}

import org.apache.spark.sql._
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


abstract class HBaseFieldResult(value: Any) extends Serializable  {
    def eval() = {
      value
    }
}
case class RowKey(value: Any) extends HBaseFieldResult (value: Any) with Serializable
case class ColumnValue(value: Any) extends HBaseFieldResult  (value: Any) with Serializable


class RowKeyResolver(result: Result, resultType: String) extends Serializable  {
  def apply() = {
    val value = resultType match {
      case "string" =>
        result.getRow.map(_.toChar).mkString
      case "int" =>
        result  .getRow.map(_.toChar).mkString.toInt
      case "long" =>
        result.getRow.map(_.toChar).mkString.toLong
    }
    RowKey(value)
  }
}
class ColumnResolver(result: Result, columnFamily: String, columnName: String, resultType: String) extends Serializable  {
  def apply() = {
    val value = resultType match {
      case "string" =>
        result.getValue(columnFamily.getBytes,columnName.getBytes).map(_.toChar).mkString
      case "int" =>
        result.getValue(columnFamily.getBytes,columnName.getBytes).map(_.toChar).mkString.toInt
      case "long" =>
        result.getValue(columnFamily.getBytes,columnName.getBytes).map(_.toChar).mkString.toLong
    }
    ColumnValue(value)
  }
}
/**
 *  CREATE TEMPORARY TABLE hbaseTable
      USING com.shengli.spark.hbase
      OPTIONS (
        registerTableSchema   '(rowkey string, f1col1 string, f1col2:string)',
        externalTableName    'test',
        externalTableSchema '(rowkey:rowkey string, f1:col1 string, f1:col2 string)'
      )
 */
case class HBaseRelation(hbaseProps: Map[String,String])(@transient val sqlContext: SQLContext) extends TableScan with Serializable {
  val externalTableName =  hbaseProps.getOrElse("externalTableName", sys.error("not valid schema"))
  val externalTableSchema =  hbaseProps.getOrElse("externalTableSchema", sys.error("not valid schema"))
  val registerTableSchema = hbaseProps.getOrElse("registerTableSchema", sys.error("not valid schema"))

  val hbaseTableFields = extractExternalSchema(externalTableSchema)
  val registerTableFields = extractRegisterSchema(registerTableSchema)
  val fieldsRelations = tableSchemaFieldMapping(hbaseTableFields,registerTableFields)
  val queryColumns =   getQueryTargetCloumns(hbaseTableFields)

  def isRowKey(field: HBaseSchemaField) : Boolean = {
    val cfColArray = field.fieldName.split(":",-1)
    val cfName = cfColArray(0)
    val colName =  cfColArray(1)
    if(cfName=="" && colName=="key") true else false
  }

  //eg: f1:col1  f1:col2  f1:col3  f2:col1
  def getQueryTargetCloumns(hbaseTableFields: Array[HBaseSchemaField]): String = {
    var str = ArrayBuffer[String]()
    hbaseTableFields.foreach{ field=>
         if(!isRowKey(field)) {
           str +=  field.fieldName
         }
    }
    str.mkString(" ")
  }
  lazy val schema = {
    val fields = hbaseTableFields.map{ field=>
        val name  = fieldsRelations.getOrElse(field, sys.error("table schema is not match the definition.")).fieldName
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

  def tableSchemaFieldMapping( externalHBaseTable: Array[HBaseSchemaField],  registerTable : Array[RegisteredSchemaField]): Map[HBaseSchemaField, RegisteredSchemaField] = {
       if(externalHBaseTable.length != registerTable.length) sys.error("columns size not match in definition!")
       val rs = externalHBaseTable.zip(registerTable)
       rs.toMap
  }

    /**
     * spark sql schema will be register
     *   registerTableSchema   '(rowkey string, value string, column_a string)'
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
          //note: the name of HBaseSchemaField is not splited, is like cf:col1
          HBaseSchemaField(fieldsNameTypeArray(0), fieldsNameTypeArray(1))
    }
  }

  val resolvedField = (hbaseField: HBaseSchemaField, result: Result ) => {
        val cfColArray = hbaseField.fieldName.split(":",-1)
        val cfName = cfColArray(0)
        val colName =  cfColArray(1)
        var fieldRs:HBaseFieldResult = null
        //resolve row key otherwise resolve column
        if(cfName=="" && colName=="key") {
          fieldRs = new RowKeyResolver(result, hbaseField.fieldType) with Serializable apply
        } else {
          fieldRs =  new ColumnResolver(result, cfName, colName,hbaseField.fieldType) with Serializable apply
        }
        fieldRs
  }

  // By making this a lazy val we keep the RDD around, amortizing the cost of locating splits.
  lazy val buildScan = {

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, externalTableName)
    hbaseConf.set(TableInputFormat.SCAN_COLUMNS, queryColumns);

    val hbaseRdd = sqlContext.sparkContext.newAPIHadoopRDD(
      hbaseConf,
      classOf[org.apache.hadoop.hbase.mapreduce.TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )
    //for string
    val rs = hbaseRdd.map(tuple => tuple._2).map(result => {
      //for each field , need to be resolved
      var values = new ArrayBuffer[Any]()
      val fields =  hbaseTableFields.map{field=>
        values += resolvedField(field,result).eval()
      }
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