/*
 * Copyright 2014 Sheng,Li
 */
package com.shengli.spark

import org.apache.spark.sql.{SQLContext, SchemaRDD}
import scala.collection.immutable.HashMap



package object hbase {

  abstract class SchemaField extends Serializable

   case class RegisteredSchemaField(fieldName: String, fieldType: String)  extends  SchemaField  with Serializable

   case class HBaseSchemaField(fieldName: String, fieldType: String)  extends  SchemaField  with Serializable

   case class Parameter(name: String)


  protected  val SPARK_SQL_TABLE_SCHEMA = Parameter("sparksql_table_schema")
  protected  val HBASE_TABLE_NAME = Parameter("hbase_table_name")
  protected  val HBASE_TABLE_SCHEMA = Parameter("hbase_table_schema")


  /**
   * Adds a method, `hbaseTable`, to SQLContext that allows reading data stored in hbase table.
   */
  implicit class HBaseContext(sqlContext: SQLContext) {
    def hbaseTable(sparksqlTableSchema: String, hbaseTableName: String, hbaseTableSchema: String) = {
      var params = new HashMap[String, String]
      params += ( SPARK_SQL_TABLE_SCHEMA.name -> sparksqlTableSchema)
      params += ( HBASE_TABLE_NAME.name -> hbaseTableName)
      params += ( HBASE_TABLE_SCHEMA.name -> hbaseTableSchema)
      sqlContext.baseRelationToSchemaRDD(HBaseRelation(params)(sqlContext))
    }
  }

//  implicit class HBaseSchemaRDD(schemaRDD: SchemaRDD) {
//    def saveIntoTable(tableName: String): Unit = ???
//  }
}
