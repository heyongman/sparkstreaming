package com.he.onlineOrder.utils

/**
  * 销售订单实体类（Scala 中样例类Case Class)
  * @param orderId
  *                订单ID
  * @param provinceId
  *                   省份ID
  * @param orderPrice
  *                   订单金额
  */
case class SaleOrder(orderId: String, provinceId: Int, orderPrice: Float)
