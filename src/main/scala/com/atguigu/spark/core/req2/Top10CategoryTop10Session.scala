package com.atguigu.spark.core.req2

import com.atguigu.spark.bean.CategoryCountInfo
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.util.control.Breaks._

// 需求二 ： Top10 热门品类的Top10 活跃Session
object Top10CategoryTop10Session {

    def main(args: Array[String]): Unit = {
        import com.atguigu.spark.accumulator.MyCategoryCountAccumulator
        import com.atguigu.spark.bean.UserVisitAction

        import scala.collection.{immutable, mutable}

        // TODO 1. 创建sparkconf 配置对象
        val conf: SparkConf = new SparkConf().setAppName("request1").setMaster("local[*]")
        // TODO 2. 创建spark环境连接对象
        val sc: SparkContext = new SparkContext(conf)

        // TODO 3. 获取原始数据信息， 将一行行的数据转换成样例类对象
        val dataRDD: RDD[String] = sc.textFile("file/user_visit_action.txt")
        val actionRDD: RDD[UserVisitAction] = dataRDD.map(line => { // 这个actionRDD就是原始数据的对象，（当前数据转成对象数据）
            import com.atguigu.spark.bean.UserVisitAction
            val datas: Array[String] = line.split("\t")
            UserVisitAction( // 构建原始数据对象：（因为样例类可以不用new就可以直接构建对象）
                datas(0),
                datas(1).toLong,
                datas(2),
                datas(3).toLong,
                datas(4),
                datas(5),
                datas(6).toLong,
                datas(7).toLong,
                datas(8),
                datas(9),
                datas(10),
                datas(11),
                datas(12).toLong
            )
        })

        // TODO 4. 将原始数据进行循环迭代，通过累加器进行聚合
        val accumulator: MyCategoryCountAccumulator = new MyCategoryCountAccumulator
        sc.register(accumulator)

        actionRDD.foreach(
            action => {
                accumulator.add(action)
            }
        )

        // TODO 5. 使用累加器聚合完成以后取出来值
        // (鞋 - click ), 100)
        // (鞋 - order ), 100)
        // (鞋 - pay ), 100)
        val accumulatorValue: mutable.HashMap[(String, String), Long] = accumulator.value

        // TODO 6. 将累计器的结果根据品类进行分组
        val stringToTupleToLong: Map[String, mutable.HashMap[(String, String), Long]] = accumulatorValue.groupBy(kv => kv._1._1)

        // TODO 7. 将分组以后的数据转换为样例类对象，方便实用
        val infos: immutable.Iterable[CategoryCountInfo] = stringToTupleToLong.map {
            case (cid, map) => {
                CategoryCountInfo(cid, map.getOrElse((cid, "click"), 0L),
                    map.getOrElse((cid, "order"), 0L),
                    map.getOrElse((cid, "pay"), 0L))
            }
        }

        // TODO 7. 降序排序
        val top10CategoryInfos: List[CategoryCountInfo] = infos.toList.sortWith(
            (left, right) => {
                if (left.clickCount > right.clickCount) {
                    true
                } else if (left.clickCount == right.clickCount) {
                    if (left.orderCount > right.orderCount) {
                        true
                    } else if (left.orderCount == right.orderCount) {
                        if (left.payCount > right.payCount) {
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
        ).take(10)

        // TODO 将前10品类的对象转换结构（Info => Id）
        val ids: List[String] = top10CategoryInfos.map(info => info.categoryId)
        //val value: Broadcast[List[String]] = sc.broadcast(ids)

        // ************************** 需求二 *****************************************
        // TODO 将原始数据进行筛选过滤，保留前10品类的数据, 而且我们只需要点击的数据，不需要下单或者其他的数据
        val filterRDD: RDD[UserVisitAction] = actionRDD.filter(action => {
            if (action.click_category_id != -1) {
                ids.contains(action.click_category_id.toString)
            } else {
                false
            }

            /*var flg = false
            breakable {
                top10CategoryInfos.foreach(
                    info => {
                        if (action.click_category_id.toString == info.categoryId) {
                            flg = true
                            break
                        }
                    }
                )
            }
            flg*/
        })

        // TODO  将过滤以后的数据进行结构的转换   UserVisitAction => (sessionId_categoryId,click)
        val sessionAndCategoryToOneRDD: RDD[(String, Long)] = filterRDD.map(filter => (filter.session_id + "_" + filter.click_category_id, 1L))

        // TODO  将转换结构以后的数据分组聚合在一起  (reduceByKey是两两进行聚合的)
        val sessionAndCategoryToSumRDD: RDD[(String, Long)] = sessionAndCategoryToOneRDD.reduceByKey(_ + _)

        // TODO  将聚合后的结果进行结构的转换(sessionId_categoryId, click) ==> (categoryId, (sessionId, clickSum))
        /*val categoryToSessionAndSumRDD: RDD[(String, (String, Long))] = sessionAndCategoryToSumRDD.map(line => {
            val strings: Array[String] = line._1.split("_")
            (strings(1), (strings(0), line._2))
        })*/

        val categoryToSessionAndSumRDD: RDD[(String, (String, Long))] = sessionAndCategoryToSumRDD.map {
            case (k, sum) => {
                val strings: Array[String] = k.split("_")
                (strings(1), (strings(0), sum))
            }
        }

        // TODO  将转换结构的数据进行分组
        val groupRDD: RDD[(String, Iterable[(String, Long)])] = categoryToSessionAndSumRDD.groupByKey()

        // TODO 将分组后的数据进行排序（降序）取前10,    现在不关心key，只关心value，使用mapValues
        val result: RDD[(String, List[(String, Long)])] = groupRDD.mapValues(datas => {
            datas.toList.sortWith(
                (left, right) => {
                    if (left._2 > right._2){
                        true
                    }else {
                        false
                    }
                }
            ).take(10) // 排序以后取10个
        })

        result.foreach(println)

        // TODO  最后一步： 释放链接
        sc.stop()


    }
}
