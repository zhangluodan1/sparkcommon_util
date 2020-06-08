package com.bi.util.spark

import com.bi.util.dim.DimUtil
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

object SparkUtil {

  def createSparkSessionEnableHive(appNmae:String,master:String,hiveUrl:String,sparkSqlShufflePartition:String):SparkSession={
    val conf = new SparkConf().setAppName(appNmae)
      .setMaster(master)
    val spark = SparkSession.builder().config(conf)
      .config("hive.metastore.uris", hiveUrl)
      .config("spark.sql.shuffle.partitions",sparkSqlShufflePartition.toInt)//设置并行度50
      .config("spark.debug.maxToStringFields",100)
      .enableHiveSupport()
      .getOrCreate()
    spark
  }
  def createSparkSession(appNmae:String,master:String,sparkSqlShufflePartition:String):SparkSession={
    val conf = new SparkConf().setAppName(appNmae)
      .setMaster(master)
    val spark = SparkSession.builder().config(conf)
      .config("spark.sql.shuffle.partitions",50)//设置并行度50
      .config("spark.debug.maxToStringFields",100)
      .getOrCreate()
    spark
  }
  def mysqldFtoPutRDD(dataDF:DataFrame,columnFamily:String) : RDD[(ImmutableBytesWritable, Put)]={
    //todo 获取DF的列名数组
    val allRetentionColumns: Array[String] = dataDF.columns
    val retentionColumns: Array[String] = allRetentionColumns.drop(6)
    println(retentionColumns.toList.toString())
    //todo 结果集数据格式转换成RDD[(ImmutableBytesWritable, Put)]
    val resultRDD: RDD[(ImmutableBytesWritable, Put)] = dataDF.rdd.repartition(10).map(row => {
      //todo 工具类获取rowkey
      val rowkey: String = DimUtil.initDim(row.getAs[String]("createtime"),
        row.getAs[String]("productid"),
        row.getAs[String]("operationlineid"),
        row.getAs[String]("advertisementid"),
        row.getAs[String]("cpsid"),
        row.getAs[String]("localeid"))
//      println(rowkey)
      //todo 创建put对象
      val put: Put = new Put(Bytes.toBytes(rowkey))

      //todo 向put中放入报表标志字段
      rowkeyToPut(rowkey,columnFamily,put)
      //todo 向put中放入所有字段值
      //获取map类型的本条数据（key和value）
      val kvString: Map[String, Any] = row.getValuesMap[Any](retentionColumns)
      dataToPut(kvString,columnFamily,put)
      (new ImmutableBytesWritable, put)
    })
    //todo 返回RDD[(ImmutableBytesWritable, Put)]
    resultRDD
  }
  def dFtoPutRDD(dataDF:DataFrame,columnFamily:String) : RDD[(ImmutableBytesWritable, Put)]={
    //todo 获取DF的列名数组
    val allRetentionColumns: Array[String] = dataDF.columns
    val retentionColumns = allRetentionColumns.drop(7)
    println(retentionColumns.toList.toString())

    //todo 结果集数据格式转换成RDD[(ImmutableBytesWritable, Put)]
    val resultRDD: RDD[(ImmutableBytesWritable, Put)] = dataDF.rdd.repartition(10).map(row => {
      //todo 工具类获取rowkey
      val rowkey: String = DimUtil.initDim(row.getAs[String]("createtime"),
        row.getAs[String]("productid"),
        row.getAs[String]("operationlineid"),
        row.getAs[String]("channelid"),
        row.getAs[String]("subchannelid"),
        row.getAs[String]("localeid"))
      //todo 创建put对象
      val put: Put = new Put(Bytes.toBytes(rowkey))

      //todo 向put中放入报表标志字段
        rowkeyToPut(rowkey,columnFamily,put)
      //todo 向put中放入所有字段值
      //获取map类型的本条数据（key和value）
      val kvString: Map[String, Any] = row.getValuesMap[Any](retentionColumns)
      dataToPut(kvString,columnFamily,put)

      (new ImmutableBytesWritable, put)
    })
    //todo 返回RDD[(ImmutableBytesWritable, Put)]
    resultRDD
  }
  def rowkeyToPut(rowkey:String,columnFamily:String,put:Put): Unit ={
    val types = ArrayBuffer[String]("name","type",
                                      "createtime","productid","operationlineid",
                                      "channelid","subchannelid","localeid")
    val values = rowkey.split('|')
    for(i <- 0 to values.length -1){
      if(values(i) == null){
        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(types(i)),"_".getBytes)
      }else{
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(types(i)), Bytes.toBytes(values(i)))
      }
    }
    put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes("devicegroupid"),Bytes.toBytes("_"))
  }
  def dataToPut(kvString: Map[String, Any],columnFamily:String,put:Put): Unit ={
    //todo 遍历kvMap，数据放入put对象中
    for ((k, v) <- kvString) {
      if (v == null) {
        put.addColumn(columnFamily.getBytes, Bytes.toBytes(k), "".getBytes)
      } else {
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(k), Bytes.toBytes(v.toString))
      }
    }
  }
}
