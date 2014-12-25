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
