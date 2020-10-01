package com.atguigu.spark.bean

/**
 * 为了让结果用来更加方便，这里添加品类点击信息的样例类。 （最终的结果需要封装的类）
 * @param categoryId    品类
 * @param clickCount    点击数量
 * @param orderCount    订单数量
 * @param payCount      支付数量
 */
case class CategoryCountInfo(categoryId: String,
                             var clickCount: Long,
                             var orderCount: Long,
                             var payCount: Long)
