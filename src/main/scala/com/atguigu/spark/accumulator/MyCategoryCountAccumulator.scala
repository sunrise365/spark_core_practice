package com.atguigu.spark.accumulator
import com.atguigu.spark.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 * HashMap中的参数是：  （品类id，事件类型），发生的总数
 */
class MyCategoryCountAccumulator extends AccumulatorV2[UserVisitAction,mutable.HashMap[(String,String),Long]]{

    // 结果需要map，需要先定义一个Map
    var map = new mutable.HashMap[(String,String),Long]()

    // 累加器是否初始化，（为空的话就是初始化了）
    override def isZero: Boolean = {
        map.isEmpty
    }

    // 复制累加器
    override def copy(): AccumulatorV2[UserVisitAction, mutable.HashMap[(String, String), Long]] = {
        new MyCategoryCountAccumulator()
    }

    // 重置累计器,(清空以后再判断是否初始化)
    override def reset(): Unit = {
        map.clear()
    }

    override def add(v: UserVisitAction): Unit = {
        if (v.click_category_id != -1){    // 点击事件
            val key: (String, String) = (v.click_category_id.toString, "click")
            // map.update(key,map.getOrElse(key,0L) + 1L)
            map(key) = map.getOrElse(key,0L) + 1L
        } else if (v.order_category_ids != "null"){     // 下单事件
            val cids: Array[String] = v.order_category_ids.split(",")
            for (id <- cids) {
                val key: (String, String) = (id, "order")
                map(key) = map.getOrElse(key,0L) + 1L
            }
        } else if (v.pay_category_ids != "null"){       // 支付事件
            val cids: Array[String] = v.pay_category_ids.split(",")
            cids.foreach( id => {
                val key: (String, String) = (id, "pay")
                map(key) = map.getOrElse(key,0L) + 1L
            })
        }
    }

    // 合并累加器, (这里是合并两个map。以其中一个map为中心，把另外一个map当做是集合之外的初始值， 把第一个map的每个元素和第二个map的初始值做聚合，产生新的map……)
    override def merge(other: AccumulatorV2[UserVisitAction, mutable.HashMap[(String, String), Long]]): Unit = {
        val map1 = map          // 以map1为中心
        val map2 = other.value  // 初始值

        // scala 中的两个集合的操作
        map = map1.foldLeft(map2)(  // TODO 特别注意， 最后需要把聚合的结果再交还给map
            (innerMap,kv) => {  // innerMap其实就是初始值，x集合， kv代表的是map1的每个元素， 两个聚合以后得到了新的x集合， 继续和下一个kv聚合
                innerMap(kv._1) = innerMap.getOrElse(kv._1,0L) + kv._2
                innerMap
            }
        )

        // java 的处理操作，将其中一个map进行遍历，和另外的map进行融合
//        val otherMap: mutable.Map[(String, String), Long] = other.value
//        otherMap.foreach {
//            kv => map.put(kv._1, map.getOrElse(kv._1, 0L) + kv._2)
//        }
    }

    // 获取累加器的值
    override def value: mutable.HashMap[(String, String), Long] = {
        map
    }
}
