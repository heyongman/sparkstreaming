package com.he.onlineETL

/**
  * 销售订单实体类（Scala 中样例类Case Class)
  * @param orderType
  *                  订单类型
  * @param orderId
  *                订单ID
  * @param provinceId
  *                   省份ID
  * @param orderPrice
  *                   订单金额
  */
case class ETLOrder(orderType: String, orderId: String, provinceId: Int, orderPrice: Float)
