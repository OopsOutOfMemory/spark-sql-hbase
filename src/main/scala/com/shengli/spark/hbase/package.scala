/*
 * Copyright 2014 Sheng,Li
 *
 */
package com.shengli.spark

import org.apache.spark.sql.{SQLContext, SchemaRDD}
import scala.collection.immutable.HashMap

package object hbase {

  abstract class SchemaField

  case class RegisteredSchemaField(fieldName: String, fieldType: String)  extends  SchemaField

  case class HBaseSchemaField(fieldName: String, fieldType: String)  extends  SchemaField
  /**
   * Adds a method, `hbaseTable`, to SQLContext that allows reading data stored in hbase table.
   */
  implicit class HBaseContext(sqlContext: SQLContext) {
    def hbaseTable(registerTableSchema: String, externalTableName: String, externalTableSchema: String) = {
      var params = new HashMap[String, String]
      params += ( "registerTableSchema" -> registerTableSchema)
      params += ( "externalTableName" -> externalTableName)
      params += ( "externalTableSchema" -> externalTableSchema)
      sqlContext.baseRelationToSchemaRDD(HBaseRelation(params)(sqlContext))
    }
  }

//  implicit class HBaseSchemaRDD(schemaRDD: SchemaRDD) {
//    def saveIntoTable(tableName: String): Unit = ???
//  }
}
