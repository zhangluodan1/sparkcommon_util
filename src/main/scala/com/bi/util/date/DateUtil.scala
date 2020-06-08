package com.bi.util.date

import java.text.SimpleDateFormat
import java.util.{Date, GregorianCalendar}

import scala.collection.mutable.ArrayBuffer

object DateUtil {
  /**
    *计算相应报表格式的当前时间
    * @param biType报表格式：H(h)-->时报  D(d)-->日报  W(W)-->周报  M(m)-->月报
    * @param preDate当前时间往前推的分钟数
    * @return
    */
  def currentMillisSubPreMinutesAndToDate(biType : String, preMin:String): String = {
    //todo 1 获取当前时间
    val currentTimeMills: Long = System.currentTimeMillis()
    //todo 2 时间参数格式转换
    val preTimeMills = currentTimeMills - preMin.toLong * 60 * 1000
    var date: String = ""
    if (biType == "H" || biType == "h") {
      date = getDateStrByDateTimeLong(preTimeMills,"yyyy-MM-dd HH")
    } else if (biType == "D" || biType == "d") {
      date = getDateStrByDateTimeLong(preTimeMills,"yyyy-MM-dd")
    } else if (biType == "W" || biType == "w") {
      println("尚未实现功能！")
    } else if (biType == "M" || biType == "m") {
      date = getDateStrByDateTimeLong(preTimeMills,"yyyy-MM")
    }
    date
  }


  /**
    *计算相应报表格式的当前时间数组
    * @param biType报表格式：H(h)-->时报  D(d)-->日报  W(W)-->周报  M(m)-->月报
    * @param preDMin当前时间往前推的分钟数
    * @return
    */
  def currentMillisSubPreMinutesAndToDateArray(biType : String, preMin:String): ArrayBuffer[String] = {
    //todo 1 获取当前时间
    val currentTimeMills: Long = System.currentTimeMillis()
    //todo 2 时间参数格式转换
    val preTimeMills = currentTimeMills - preMin.toLong * 60 * 1000
    var dateArr: ArrayBuffer[String] = new ArrayBuffer[String]()
    if (biType == "H" || biType == "h") {
      dateArr += getDateStrByDateTimeLong(preTimeMills,"yyyy-MM-dd HH")
    } else if (biType == "D" || biType == "d") {
      dateArr += getDateStrByDateTimeLong(preTimeMills,"yyyy-MM-dd")
    } else if (biType == "W" || biType == "w") {
      println("尚未实现功能！")
    } else if (biType == "M" || biType == "m") {
      dateArr += getDateStrByDateTimeLong(preTimeMills,"yyyy-MM")
    }
    dateArr
  }
  /**
    *计算时报格式的当前时间数组
    * @param preMin当前时间往前推的分钟数
    * @return
    */
  def currentMillisSubPreMinutesAndToDateArrayForHour(preMin:String): ArrayBuffer[String] = {
    //todo 1 获取当前时间
    val currentTimeMills: Long = System.currentTimeMillis()
    //todo 2 时间参数格式转换
    val preTimeMills = currentTimeMills - preMin.toLong * 60 * 1000
    var dateArr: ArrayBuffer[String] = new ArrayBuffer[String]()
    dateArr += getDateStrByDateTimeLong(preTimeMills,"yyyy-MM-dd HH")
    dateArr
  }
  /**
    *计算日报格式的当前时间数组
    * @param preMin当前时间往前推的分钟数
    * @return
    */
  def currentMillisSubPreMinutesAndToDateArrayForDay(preMin:String): ArrayBuffer[String] = {
    //todo 1 获取当前时间
    val currentTimeMills: Long = System.currentTimeMillis()
    //todo 2 时间参数格式转换
    val preTimeMills = currentTimeMills - preMin.toLong * 60 * 1000
    var dateArr: ArrayBuffer[String] = new ArrayBuffer[String]()
    dateArr += getDateStrByDateTimeLong(preTimeMills,"yyyy-MM-dd")
    dateArr
  }
  /**
    *计算月报格式的当前时间数组
    * @param preMin当前时间往前推的分钟数
    * @return
    */
  def currentMillisSubPreMinutesAndToDateArrayForMonth(preMin:String): ArrayBuffer[String] = {
    //todo 1 获取当前时间
    val currentTimeMills: Long = System.currentTimeMillis()
    //todo 2 时间参数格式转换
    val preTimeMills = currentTimeMills - preMin.toLong * 60 * 1000
    var dateArr: ArrayBuffer[String] = new ArrayBuffer[String]()
    dateArr += getDateStrByDateTimeLong(preTimeMills,"yyyy-MM")
    dateArr
  }


  /**
    * 处理开始时间和结束时间成对应报表格式的dateArrayBuffer
   * @param biType 报表格式：H(h)-->时报  D(d)-->日报  W(W)-->周报  M(m)-->月报
    * @param startTimeWithNoBlank 无空格的开始时间
    * @param endTimeWithNoBlank 无空格的结束时间
    * @return
    */
  def startAndEndTimeToDateArray(biType:String,startTimeWithNoBlank:String,endTimeWithNoBlank:String):ArrayBuffer[String]={
    //todo 创建存放时间的字符串数组
    var dateArrBuff: ArrayBuffer[String] = new ArrayBuffer[String]()

    //todo 时间参数格式转换
    if (biType == "H" || biType == "h") {//todo 处理时报
      dateArrBuff = startAndEndTimeToDateArrayForHour(startTimeWithNoBlank,endTimeWithNoBlank)
    } else if (biType == "D" || biType == "d") { //todo 处理日报
      dateArrBuff = startAndEndTimeToDateArrayForDay(startTimeWithNoBlank,endTimeWithNoBlank)
    } else if (biType == "W" || biType == "w") { //todo 处理周报
      println("尚未实现功能！")
    } else if (biType == "M" || biType == "m") { //todo 处理月报
      dateArrBuff = startAndEndTimeToDateArrayForMonth(startTimeWithNoBlank,endTimeWithNoBlank)
    }
    dateArrBuff
  }
  /**
    * 处理开始时间和结束时间成时报格式的dateArrayBuffer
    * @param startTimeWithNoBlank 无空格的开始时间
    * @param endTimeWithNoBlank 无空格的结束时间
    * @return
    */
  def startAndEndTimeToDateArrayForHour(startTimeWithNoBlank:String,endTimeWithNoBlank:String):ArrayBuffer[String]={
    //todo 创建存放时间的字符串数组
    val dateArrBuff: ArrayBuffer[String] = new ArrayBuffer[String]()
    //开始和结束时间字符串加空格
    val startTime:String=startTimeWithNoBlank.take(10) + " " + startTimeWithNoBlank.takeRight(2)
    println(startTime)
    val endTime:String=endTimeWithNoBlank.take(10) + " " + endTimeWithNoBlank.takeRight(2)
    println(endTime)
    //开始和结束时间转换成Long类型时间戳
    val startTimeMills: Long = getTimeMillsByDateStr(startTime,"yyyy-MM-dd HH")
    println(startTimeMills)
    val endTimeMills: Long = getTimeMillsByDateStr(endTime,"yyyy-MM-dd HH")
    println(endTimeMills)
    //计算开始和结束时间中间的所有符合date，添加到dateArr
    var dateTimeMills = startTimeMills
    while(dateTimeMills <= endTimeMills){
      val dateStr: String = getDateStrByDateTimeLong(dateTimeMills,"yyy-MM-dd HH")
      dateArrBuff += dateStr
      dateTimeMills += 60*60*1000
    }
    dateArrBuff
  }
  /**
    * 处理开始时间和结束时间成日报格式的dateArrayBuffer
    * @param startTime 开始时间
    * @param endTime 结束时间
    * @return
    */
  def startAndEndTimeToDateArrayForDay(startTime:String,endTime:String):ArrayBuffer[String]={
    //todo 创建存放时间的字符串数组
    val dateArrBuff: ArrayBuffer[String] = new ArrayBuffer[String]()
    //开始和结束时间转换成Long类型时间戳
    val startTimeMills: Long = getTimeMillsByDateStr(startTime,"yyyy-MM-dd")
    println(startTimeMills)
    val endTimeMills: Long = getTimeMillsByDateStr(endTime,"yyyy-MM-dd")
    println(endTimeMills)
    //计算开始和结束时间中间的所有符合date，添加到dateArr
    var dateTimeMills = startTimeMills
    while(dateTimeMills <= endTimeMills){
      val dateStr: String = getDateStrByDateTimeLong(dateTimeMills,"yyy-MM-dd")
      dateArrBuff += dateStr
      dateTimeMills += 24*60*60*1000
    }
    dateArrBuff
  }
  /**
    * 处理开始时间和结束时间成月报格式的dateArrayBuffer
    * @param startTime 开始时间
    * @param endTime 结束时间
    * @return
    */
  def startAndEndTimeToDateArrayForMonth(startTime:String,endTime:String):ArrayBuffer[String]={
    //todo 创建存放时间的字符串数组
    val dateArrBuff: ArrayBuffer[String] = new ArrayBuffer[String]()
    val startYear = startTime.take(4).toInt
    val startMonth = startTime.takeRight(2).toInt
    val endYear = endTime.take(4).toInt
    val endMonth = endTime.takeRight(2).toInt
    val startCalendar = new GregorianCalendar(startYear,startMonth -1,1)
    val endCalendar = new GregorianCalendar(endYear,endMonth -1,1)
    val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM")
    while(startCalendar.getTimeInMillis <= endCalendar.getTimeInMillis){
      dateArrBuff += format.format(startCalendar.getTime)
      startCalendar.add(2,1)
    }
    dateArrBuff
  }

  /**
    *
    * 格式化日期，把字符串日期格式的转换为long类型
    *
    * @param dateStr
    * @param pattern
    * @return
    */
  def getTimeMillsByDateStr(dateStr:String,pattern:String):Long ={
    val format: SimpleDateFormat = new SimpleDateFormat(pattern)
    format.parse(dateStr).getTime
  }
  /**
    * 格式化日期字符串
    * @param date
    * @param pattern
    * @return
    */
  def getTimeStrByDate(date:Date,pattern:String):String ={
    val format: SimpleDateFormat = new SimpleDateFormat(pattern)
    format.format(date)
  }
  /**
    * 根据时间戳，获取日期
    * @param dateTimeStr	时间字符串
    * @param pattern	转换格式
    * @return	日期字符串
    */
  def getDateStrByDateTimeLong(dateTimeStr:Long,pattern:String):String={
    val date: Date = new Date(dateTimeStr)
    val format: SimpleDateFormat = new SimpleDateFormat(pattern)
    format.format(date)
  }
}
