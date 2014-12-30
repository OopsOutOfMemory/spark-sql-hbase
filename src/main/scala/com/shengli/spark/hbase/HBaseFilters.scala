/*
* Copyright 2014 Sheng,Li
*/
package com.shengli.spark.hbase

import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{Filter => HBaseFilter}
import org.apache.spark.sql.sources.{Filter => SourceFilter, _}
import org.apache.hadoop.hbase.filter.CompareFilter._
import org.apache.hadoop.hbase.util.Bytes
import com.shengli.spark.hbase.RegisteredSchemaField
import com.shengli.spark.hbase.HBaseSchemaField
import org.apache.spark.sql.sources.EqualTo
import org.apache.spark.Logging


object HBaseFilters extends Logging {

  def isRowKey(field: HBaseSchemaField) : Boolean = {
    val cfColArray = field.fieldName.split(":",-1)
    val cfName = cfColArray(0)
    val colName =  cfColArray(1)
    if(cfName=="" && colName=="key") true else false
  }


 def bindColumnFilters(hbaseField: HBaseSchemaField, attr:String, op:CompareOp,value: Any) : HBaseFilter = {
   val cf_column = hbaseField.fieldName.split(":",-1)
   val cf = cf_column(0)
   val column = cf_column(1)
   val filter = new SingleColumnValueFilter(
     Bytes.toBytes(cf),
     Bytes.toBytes(column),
     op,
     Bytes.toBytes(value.asInstanceOf[String])
   )
   logInfo("filter is "+ filter)
   filter
 }

  def initialFilters(scan: Scan, rawfilters: Array[SourceFilter], mapping : Map[HBaseSchemaField, RegisteredSchemaField] ) : FilterList  = {
    val list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    val reversedMapping = mapping.map(_ swap).map(kv=>(kv._1.fieldName,kv._2))
    rawfilters.foreach{
      case e @ EqualTo(attr, value) =>
        val hbaseField = reversedMapping.getOrElse(attr, sys.error("not found hbase table field in filter"))
        var filter:HBaseFilter = null
        //check if is row_key
        if(!isRowKey(hbaseField)) {
          val filter = bindColumnFilters(hbaseField, attr, CompareOp.EQUAL ,value)
          list.addFilter(filter)
        }
    }
    list
  }
}