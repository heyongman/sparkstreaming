package com.he.onlineETL

import java.util.Random

/**
  * 随机数获取
  */
object ETLRandomUtils {

  def getRandomNum(bound: Int): Int = {
    // 创建 随机序列
    val random = new Random()
    // 生成随机数字
    random.nextInt(bound)
  }
}
