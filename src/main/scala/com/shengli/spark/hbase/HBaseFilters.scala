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
import org.apache.spark.sql.sources.EqualTo
import org.apache.spark.Logging
import scala.collection.mutable.ArrayBuffer


object HBaseFilters extends Logging {

  def isRowKey(field: HBaseSchemaField) : Boolean = {
    val cfColArray = field.fieldName.split(":",-1)
    val cfName = cfColArray(0)
    val colName =  cfColArray(1)
    if(cfName=="" && colName=="key") true else false
  }


  def createColumnFilters(hbaseField: HBaseSchemaField, attr:String, op:CompareOp,value: Any) : HBaseFilter = {
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

  /**
   * Receive a list of external datasource Filters and mapping them to hbase filters
   * @param scan the scan need to query
   * @param rawfilters  filters extracted from catalyst
   * @param mapping  external mapping to catalyst table mapping
   * @return
   */
  def initialFilters(scan: Scan, rawfilters: Array[SourceFilter], mapping : Map[HBaseSchemaField, RegisteredSchemaField] ) : FilterList  = {
    var list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    val reversedMapping = mapping.map(_ swap).map(kv=>(kv._1.fieldName,kv._2))
    rawfilters.foreach{
      // = equal to
      case e @ EqualTo(attr, value) =>
        val filters = generateHBaseFiletr(attr,value,reversedMapping, CompareOp.EQUAL)
        list :: filters
      // > grater than
      case e @ GreaterThan(attr, value) =>
        val hbaseField = reversedMapping.getOrElse(attr, sys.error("not find hbase table field in filter"))
        //check if is row_key
        if(!isRowKey(hbaseField)) {
          val filter = createColumnFilters(hbaseField, attr, CompareOp.GREATER ,value)
          list.addFilter(filter)
        }
      // <  less than
      case e @ LessThan(attr, value) =>
        val hbaseField = reversedMapping.getOrElse(attr, sys.error("not find hbase table field in filter"))
        //check if is row_key
        if(!isRowKey(hbaseField)) {
          val filter = createColumnFilters(hbaseField, attr, CompareOp.LESS ,value)
          list.addFilter(filter)
        }

    }
    list
  }

  /**
   * According to Catalyst Query Filter to generate HBase Filter
   * @param catalystAttr
   * @param catalystVal
   * @param mapping
   * @param oper
   * @return
   */
  def generateHBaseFiletr(catalystAttr: String, catalystVal: Any, mapping: Map[String, HBaseSchemaField], oper: CompareOp): List[Filter] = {
    val hbaseField = mapping.getOrElse(catalystAttr, sys.error("not find hbase table field in filter"))
    val filterList = ArrayBuffer[Filter]()
    //check if is row_key
    if(!isRowKey(hbaseField)) {
      val filter = createColumnFilters(hbaseField, catalystAttr, oper ,catalystVal)
      filterList += filter
    }
    filterList.toList
  }
}