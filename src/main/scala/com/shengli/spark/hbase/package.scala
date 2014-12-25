/*
 * Copyright 2014 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
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
package com.shengli.spark

import org.apache.spark.sql.{SQLContext, SchemaRDD}
import scala.collection.immutable.HashMap

package object hbase {
  /**
   *  CREATE TEMPORARY TABLE hbaseTable
      USING com.shengli.spark.hbase
      OPTIONS (
        registerTableSchema   '(rowkey string, value string)',
        externalTableName    'test',
        externalTableSchema '(:key, cf:a)'
      )
   */

  abstract class SchemaField

  case class RegisteredSchemaField(fieldName: String, fieldType: String)  extends  SchemaField

  case class HBaseSchemaField(fieldName: String, fieldType: String)  extends  SchemaField
  /**
   * Adds a method, `hbaseFile`, to SQLContext that allows reading data stored in hbase table.
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
