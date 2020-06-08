package com.bi.util.property

import java.io.FileInputStream
import java.util.Properties


object PropertiesUtil {

  val properties: Properties = loadProperties("spark.properties")
//    val properties: Properties = loadPropertiesByDefaultPath("spark.properties")

  /**
    * 加载本地resources文件夹下的某配置文件
    * @param fileName
    * @return 配置文件中配置参数对象Properties
    */
  def loadProperties(fileName:String): Properties = {
    val properties = new Properties()
    val inputStream = this.getClass.getClassLoader.getResourceAsStream(fileName)
    properties.load(inputStream)
    properties
  }

  /**
    * 依据配置文件所在的路径创建Properties对象
 *
    * @param path 配置文件所在的路径
    * @return
    */
  def loadPropertiesByPath(path:String): Properties = {
    val properties = new Properties()
    properties.load(new FileInputStream(path))
    properties
  }

  /**
    *  依据配置文件名和默认路径创建Properties对象
    * @param fileName 配置文件名
    * @return
    */
  def loadPropertiesByDefaultPath(fileName:String):Properties ={
    val properties = new Properties()
    properties.load(new FileInputStream("/opt/longtu/tmp/" + fileName))
    properties
  }

  /**
    * 获取hbase的quorum
    * @param properties
    * @return
    */
  def getHbaseQuorum():String={
    val hbaseQuoeum: String = properties.getProperty("hbase.zookeeper.quorum")
    hbaseQuoeum
  }

  /**
    * 获取hbase的port
    * @param properties
    * @return
    */
  def getHbasePort():String={
    val hbasePort: String = properties.getProperty("hbase.zookeeper.property.clientPort")
    hbasePort
  }

  /**
    * 获取hbase的tableName
    * @param properties
    * @return
    */
  def getHbaseTableName():String={
    val hbaseTableName: String = properties.getProperty("tableName")
    hbaseTableName
  }

  /**
    * 获取hbase的columnFamily
    * @param properties
    * @return
    */
  def getHbaseColumnFamily():String={
    val hbaseColumnFamily: String = properties.getProperty("columnFamily")
    hbaseColumnFamily
  }

  /**
    * 获取sparkMaster
    * @param properties
    * @return
    */
  def getSparkMaster():String={
    val sparkMaster: String = properties.getProperty("master")
    sparkMaster
  }

  /**
    * 获取连接hive的URL地址
    * @param properties
    * @return
    */
  def getHiveUrl():String={
    val hiveUrl: String = properties.getProperty("hive.metastore.uris")
    hiveUrl
  }

  /**
    * 获取sparkSql的shuffle并行度设置值
    * @param properties
    * @return
    */
  def getSparkSQLShufflePartitions():String={
    val sparkSQLShufflePartitions: String = properties.getProperty("spark.sql.shuffle.partitions")
    sparkSQLShufflePartitions
  }
}
