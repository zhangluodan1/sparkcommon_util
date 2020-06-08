package com.bi.util.dim

object DimUtil {
  /**
    * 依据dt/productid/operationlineid/channelid/subchannelid/localeid拼接rowkey方法
    * @param dt 日期
    * @param productid 产品id
    * @param operationlineid 联运id
    * @param channelid 主渠道id
    * @param subchannelid 子渠道id
    * @param localeid 地域id
    * @return rowkey
    */
  def initDim(createtime:String,
              productid:String,
              operationlineid:String,
              channelid:String,
              subchannelid:String,
              localeid:String):String ={
    // todo 1 定义报表类型标志
    var sign = ""
    if(createtime.length == 6){ //todo dt字符串长度为7，是月报
      sign = "REPORT|MONTH"
    }else if(createtime.length == 8){  //todo dt字符串长度为10，是日报
      sign = "REPORT|DAY"
    }else if(createtime.length == 10){  //todo dt字符串长度为13，是时报
      sign = "REPORT|HOUR"
    }
    // todo 1 拼接rowkey字符串
    if(subchannelid != null && subchannelid != "" && subchannelid != "0" && subchannelid != "_"){
      sign + "|" + createtime + "|" + productid + "|" + operationlineid + "|" + channelid + "|" + subchannelid + "|" + localeid
    }else if(channelid != null && channelid != "" && channelid != "0" && channelid != "_"){
      sign + "|" + createtime + "|" + productid + "|" + operationlineid + "|" + channelid + "|" + "_" + "|" + localeid
    }else if(operationlineid != null && operationlineid != "" && operationlineid != "0" && operationlineid != "_"){
      sign + "|" + createtime + "|" + productid + "|" + operationlineid + "|" + "_" + "|" + "_" + "|" + localeid
    }else {
      sign + "|" + createtime + "|" + productid + "|" + "_" + "|" + "_" + "|" + "_"  + "|" + localeid
    }
  }
}
