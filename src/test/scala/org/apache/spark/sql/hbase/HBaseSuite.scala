package com.shengli.spark.hbase.test

import org.apache.spark.sql.test._
import org.scalatest.FunSuite
import com.shengli.spark.hbase._

/* Implicits */
import TestSQLContext._

class HBaseSuite extends FunSuite {

  test("dsl test") {

    val results = TestSQLContext.hbaseTable("(row_key string, name string, age int, job string)","people","(:key , profile:name , profile:age , career:job )").select('name).collect()
    assert(results.size === 5)
  }

  test("sql test") {
    sql(
      s"""
      |CREATE TEMPORARY TABLE hbase_people
      |USING com.shengli.spark.hbase
      |OPTIONS (
      |  sparksql_table_schema   '(row_key string, name string, age int, job string)',
      |  hbase_table_name    'people',
      |  hbase_table_schema '(:key , profile:name , profile:age , career:job )'
      |)""".stripMargin
    )
    assert( sql("SELECT * FROM hbase_people").collect().size === 5 )
  }
}
