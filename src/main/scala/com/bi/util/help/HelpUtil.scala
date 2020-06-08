package com.bi.util.help

import scala.util.control.Breaks.{break, breakable}

object HelpUtil {

  def getHelp(sign:String,paramList:List[String],explainList:List[String])={
    breakable{
      if(sign.toUpperCase == "HELP"){
        println("进入help选项！")
        for(i <- 1 to paramList.size) {
        println(paramList(i - 1)+ ":" )
        println("\t\t\t\t\t" + explainList(i - 1))
        }
      }else {
        println("请按照正确参数格式，如：help 输入参数")
      }
      break
    }

  }
}
