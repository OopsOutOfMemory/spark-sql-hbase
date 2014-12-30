/*
* Copyright 2014 Sheng, Li
*/
package com.shengli.spark.hbase

import java.io.Serializable

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources.PrunedFilteredScan
import org.apache.spark.sql.sources._
import scala.collection.immutable.{HashMap, Map}
import org.apache.hadoop.hbase.client.{Result, Scan, HTable, HBaseAdmin}
import org.apache.spark.sql._
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.Logging
import org.apache.spark._
import org.apache.hadoop.hbase.util.Bytes
import java.io.{DataOutputStream, ByteArrayOutputStream}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Base64



object Resolver extends  Serializable {

  def resolve (hbaseField: HBaseSchemaField, result: Result ): Any = {
    val cfColArray = hbaseField.fieldName.split(":",-1)
    val cfName = cfColArray(0)
    val colName =  cfColArray(1)
    var fieldRs: Any = null
    //resolve row key otherwise resolve column
    if(cfName=="" && colName=="key") {
      fieldRs = resolveRowKey(result, hbaseField.fieldType)
    } else {
      fieldRs =  resolveColumn(result, cfName, colName,hbaseField.fieldType)
    }
    fieldRs
  }

  def resolveRowKey (result: Result, resultType: String): Any = {
     val rowkey = resultType match {
      case "string" =>
        result.getRow.map(_.toChar).mkString
      case "int" =>
        result  .getRow.map(_.toChar).mkString.toInt
      case "long" =>
        result.getRow.map(_.toChar).mkString.toLong
    }
    rowkey
  }

  def resolveColumn (result: Result, columnFamily: String, columnName: String, resultType: String): Any = {
    val column = resultType match {
      case "string" =>
        result.getValue(columnFamily.getBytes,columnName.getBytes).map(_.toChar).mkString
      case "int" =>
        result.getValue(columnFamily.getBytes,columnName.getBytes).map(_.toChar).mkString.toInt
      case "long" =>
        result.getValue(columnFamily.getBytes,columnName.getBytes).map(_.toChar).mkString.toLong
    }
    column
  }
}

/**
   val hbaseDDL = s"""
      |CREATE TEMPORARY TABLE hbase_people
      |USING com.shengli.spark.hbase
      |OPTIONS (
      |  sparksql_table_schema   '(row_key string, name string, age int, job string)',
      |   hbase_table_name     'people',
      | hbase_table_schema '(:key , profile:name , profile:age , career:job )'
      |)""".stripMargin
 */
case class HBaseRelation(@transient val hbaseProps: Map[String,String])(@transient val sqlContext: SQLContext) extends PrunedFilteredScan with Serializable with Logging{

  val hbaseTableName =  hbaseProps.getOrElse("hbase_table_name", sys.error("not valid schema"))
  val hbaseTableSchema =  hbaseProps.getOrElse("hbase_table_schema", sys.error("not valid schema"))
  val registerTableSchema = hbaseProps.getOrElse("sparksql_table_schema", sys.error("not valid schema"))


  val tempHBaseFields = extractHBaseSchema(hbaseTableSchema) //do not use this, a temp field
  val registerTableFields = extractRegisterSchema(registerTableSchema)
  val tempFieldRelation = tableSchemaFieldMapping(tempHBaseFields,registerTableFields)

  val hbaseTableFields = feedTypes(tempFieldRelation)
  val fieldsRelations =  tableSchemaFieldMapping(hbaseTableFields,registerTableFields)
  val queryColumns =  getQueryTargetCloumns(hbaseTableFields)

  def  feedTypes( mapping: Map[HBaseSchemaField, RegisteredSchemaField]) :  Array[HBaseSchemaField] = {
         val hbaseFields = mapping.map{
           case (k,v) =>
               val field = k.copy(fieldType=v.fieldType)
               field
        }
        hbaseFields.toArray
  }

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

  def tableSchemaFieldMapping(externalHBaseTable: Array[HBaseSchemaField],  registerTable : Array[RegisteredSchemaField]): Map[HBaseSchemaField, RegisteredSchemaField] = {
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

  //externalTableSchema '(:key , f1:col1 )'
  def extractHBaseSchema(externalTableSchema: String) : Array[HBaseSchemaField] = {
        val fieldsStr = externalTableSchema.trim.drop(1).dropRight(1)
        val fieldsArray = fieldsStr.split(",").map(_.trim)
        fieldsArray.map(fildString => HBaseSchemaField(fildString,""))
  }

  def convertScanToString(scan: Scan): String = {
    val out: ByteArrayOutputStream = new ByteArrayOutputStream
    val dos: DataOutputStream = new DataOutputStream(out)
    scan.write(dos)
    Base64.encodeBytes(out.toByteArray)
  }
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, hbaseTableName)
    hbaseConf.set(TableInputFormat.SCAN_COLUMNS, queryColumns);

    val filters = HBaseFilters.initialFilters(filters, fieldsRelations)

    val scan = new Scan()
    scan.setFilter(filters)
    scan.setStartRow(Bytes.toBytes("rowkey002"))
    scan.setStopRow(Bytes.toBytes("rowkey005"))

    val hbaseRdd = sqlContext.sparkContext.newAPIHadoopRDD(
      hbaseConf,
      classOf[org.apache.hadoop.hbase.mapreduce.TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )

    val rs = hbaseRdd.map(tuple => tuple._2).map(result => {
      var values = new ArrayBuffer[Any]()
      hbaseTableFields.foreach{field=>
        values += Resolver.resolve(field,result)
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