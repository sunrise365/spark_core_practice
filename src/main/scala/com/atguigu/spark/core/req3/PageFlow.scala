package com.atguigu.spark.core.req3

import com.atguigu.spark.bean.CategoryCountInfo
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.util.control.Breaks._

// 需求三 ： 页面单跳转换率
object PageFlow {

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

        // TODO 需要统计页面单跳转换率的页面路径
        val pageflows = List(1,2,3,4,5,6)
        // List(  (1-2),(2-3),(3-4) )
        val pageflowZip: List[(Int, Int)] = pageflows.zip(pageflows.tail)
        val pageflowStringList: List[String] = pageflowZip.map(t=>t._1 + "-"+t._2)

        // 分母------分母是可以在这里直接过滤的，但是分子不可以，比如 3,20,4,  如果去掉20，会认为直接从3跳转到4，所以分子开始的形式是 List(  (1-2),(2-3),(3-4) )
        // TODO 将数据转换成结构 （ pageid, 1L）
        val filterRDD: RDD[UserVisitAction] = actionRDD.filter(action=>pageflows.contains(action.page_id.toInt))
        val pageIdToOneRDD: RDD[(Long, Long)] = filterRDD.map(action => (action.page_id, 1L))
        val pageIdToSumMap: collection.Map[Long, Long] = pageIdToOneRDD.countByKey() // 1. 可以使用reduceByKey


        // 分子
        // TODO 将原始数据根据Sessionid进行分组
        val sessionGroupRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(action => action.session_id)

        // TODO 将分组后的数据根据页面访问时间进行排序（升序）
        val sessionToPageFlows: RDD[(String, List[(String, Long)])] = sessionGroupRDD.mapValues(datas => {
            val actions: List[UserVisitAction] = datas.toList.sortWith(
                (left, right) => {
                    left.action_time < right.action_time
                }
            )

            // 1,2,3,4,5,6,7
            val pages: List[Long] = actions.map(_.page_id)

            // 1,2,3,4,5,6,7
            // 实际需要的数据类型： (1-2,1),(2-3,1),(3-4,1),(4-5,1),(5-6,1),(6-7,1)

            //  方式一： 使用滑窗,步长默认为1
            /*val pagesIterator: Iterator[List[Long]] = pages.sliding(2)
            while (pagesIterator.hasNext){
                val pageTwo: List[Long] = pagesIterator.next()
                (pageTwo(0) + "-" + pageTwo(1),1)
            }*/

            /*
            方式二：拉链
                    [1,2,3,4,5,6,7]
                    [2,3,4,5,6,7]
             */
            // 得到的数据类型是： (1-2),(2-3),(3-4),(4-5),(5-6),(6-7)
            val pageIdToPageIds: List[(Long, Long)] = pages.zip(pages.tail)
            // TODO 新增： 在这里对分子尽心过滤,(最好使用模式匹配， 使用_1   _2 很多时候不知道具体的含义)
            // val pageFlowFilter: List[(Long, Long)] = pageIdToPageIds.filter(flow => pageflowStringList.contains(flow._1 + "-" + flow._2))
            val pageFlowFilter: List[(Long, Long)] = pageIdToPageIds.filter {
                case (pid1, pid2) => {
                    pageflowStringList.contains(pid1 + "-" + pid2)
                }
            }
            val pageFlowToOne: List[(String, Long)] = pageFlowFilter.map(pageTwo => (pageTwo._1 + "-" + pageTwo._2, 1L))
            pageFlowToOne
        })


        // TODO  将拉链后的数据进行结构的转换（session, Iterator((pageid1-pageid2, 1))）=> Iterator((pageid1-pageid2, 1))
        val pageFlowRDD: RDD[List[(String, Long)]] = sessionToPageFlows.map {
            case (session, datas) => {
                datas
            }
        }

        // TODO 将转换结构后的数据进行扁平化操作,  （将原来一个集合的整体的数据装换成一个个的点击次数的集合）
        // (1-2,1), (2-3,1), (1-2,1)
        val pageFlowSingleRDD: RDD[(String, Long)] = pageFlowRDD.flatMap(datas => datas)

        // TODO 将扁平化后的数据进行分组聚合
        // (1-2,100), (2-3,50)
        val pageflowToCountRDD: collection.Map[String, Long] = pageFlowSingleRDD.countByKey()

        // TODO 分子 / 分母
        pageflowToCountRDD.foreach{
            case (pageFlow, v) => {
                val strings: Array[String] = pageFlow.split("-")
                val fenmu : Long = pageIdToSumMap.getOrElse(strings(0).toLong,1L)
                println(pageFlow + "=" + (v.toDouble / fenmu))
                (pageFlow,v / fenmu)
            }
        }



        // TODO  最后一步： 释放链接
        sc.stop()


    }
}
