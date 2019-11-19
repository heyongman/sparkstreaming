package com.he.onlineOrder.utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
  * 单例模式，Lazy方式创建ObjectMapper实例对象
  */
object ObjectMapperSingleton {

  // transient是类型修饰符，只能用来修饰字段，当对象序列化的过程中，变量不会被序列化
  @transient private var instance: ObjectMapper = _

  def getInstance(): ObjectMapper = {
    if(instance == null){
      instance = new ObjectMapper()
      instance.registerModule(DefaultScalaModule)
    }
    instance
  }

}
