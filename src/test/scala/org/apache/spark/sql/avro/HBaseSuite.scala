/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.shengli.spark.hbase.test

import org.apache.spark.sql.test._
import org.scalatest.FunSuite

/* Implicits */
import TestSQLContext._

class HBaseSuite extends FunSuite {

  test("dsl test") {
    val results = TestSQLContext.hbaseTable("(rowkey string, value string)","test","(rowkey:rowkey string,cf:a string)").select('value).count.collect()
    assert(results.size === 5)
  }

  test("sql test") {
    sql(
      s"""
      |CREATE TEMPORARY TABLE hbase_people
      |USING com.shengli.spark.hbase
      |OPTIONS (
      |  registerTableSchema   '(row_key string, name string)',
      |  externalTableName    'people',
      |  externalTableSchema '(rowkey:rowkey string, profile:name string)'
      |)""".stripMargin

    assert(sql("SELECT * FROM hbase_people").collect().size === 5)  )
  }
}
