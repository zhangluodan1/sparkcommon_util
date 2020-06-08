package com.bi.util.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf

object HBaseUtil extends Serializable {

  /**
   * @features 将RDD中的数据插入到hbase中
   * @param putRDD:插入到hbase的rdd数据
   * @param tbName：插入hbase的表名
   * @param zookeeperQuorum：HBase的zookeeper节点的ip
   * @param clientPort：zookeeper的端口
   */
  def insertRDD2Hbase(
    putRDD: RDD[(ImmutableBytesWritable, Put)],
    tbName: String, zookeeperQuorum: String, clientPort: String) {
    val jobConf = getJobConf(zookeeperQuorum, clientPort)
    val conn = ConnectionFactory.createConnection(jobConf)
    insertRDD2Hbase(putRDD, tbName, jobConf)
  }
  /**
   * @features 将RDD中的数据插入到hbase中
   * @param putRDD:插入到hbase的rdd数据
   * @param tbName：插入hbase的表名
   * @param jobConf：hbase的配置信息
   */
  def insertRDD2Hbase(putRDD: RDD[(ImmutableBytesWritable, Put)], tbName: String, jobConf: JobConf) {
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tbName)
    putRDD.saveAsHadoopDataset(jobConf)
    println("一批数据已成功保存到hbase")
  }
  /**
   * @features 获取JobConf对象
   * @param zookeeperQuorum：HBase的zookeeper节点的ip
   * @param clientPort：zookeeper的端口
   * @return JobConf
   */
  def getJobConf(zookeeperQuorum: String, clientPort: String): JobConf = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zookeeperQuorum)
    conf.set("hbase.zookeeper.property.clientPort", clientPort)
    new JobConf(conf)
  }
  /**
   * @features 获取hbase连接
   * @param zookeeperQuorum：HBase的zookeeper节点的ip
   * @param clientPort：zookeeper的端口
   * @return HBase的连接
   */
  def getConnection(zookeeperQuorum: String, clientPort: String): Connection = {
    ConnectionFactory.createConnection( getJobConf(zookeeperQuorum,clientPort))
  }



}