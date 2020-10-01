package com.atguigu.spark.core.req1

/**
 * 需求一： top10 热门品类
 */
object Request1Top10Tradition {

    import com.atguigu.spark.bean.CategoryCountInfo
    import org.apache.spark.rdd.RDD
    import org.apache.spark.{SparkConf, SparkContext}

    import scala.collection.mutable.ListBuffer

    def main(args: Array[String]): Unit = {
        import com.atguigu.spark.bean.UserVisitAction

        // TODO 1. 创建sparkconf 配置对象
        val conf: SparkConf = new SparkConf().setAppName("request1").setMaster("local[*]")
        // TODO 2. 创建spark环境连接对象
        val sc: SparkContext = new SparkContext(conf)

        // TODO 3. 获取原始数据信息， 将一行行的数据转换成样例类对象
        val dataRDD: RDD[String] = sc.textFile("file/user_visit_action.txt")
        val actionRDD: RDD[UserVisitAction] = dataRDD.map(line => {     // 这个actionRDD就是原始数据的对象，（当前数据转成对象数据）
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

        // TODO 4.1 过滤数据， 只留下点击、下单、支付
        // TODO 4.2 将下单和支付的数据进行扁平化操作。（比如一次下单可能同时下单多个商品，这些商品用逗号隔开的，
        //         id1,id2,id3 => (id1,1),(id2,1),(id3,1),          可以在扁平化的同时进行过滤，对源数据对象进行模式匹配
        val flatMapRDD: RDD[CategoryCountInfo] = actionRDD.flatMap(action => {
            action match {
                case UserVisitAction( // 模式匹配： 匹配这个数据的对象，将不需要的数据过滤
                date: String,
                user_id: Long,
                session_id: String,
                page_id: Long,
                action_time: String,
                search_keyword: String,
                click_category_id: Long,
                click_product_id: Long,
                order_category_ids: String,
                order_product_ids: String,
                pay_category_ids: String,
                pay_product_ids: String,
                city_id: Long
                ) if (click_category_id != -1) => List(CategoryCountInfo(click_category_id.toString, 1, 0, 0)) // if模式守卫，留下id不是-1说明是点击事件

                case UserVisitAction(
                date: String,
                user_id: Long,
                session_id: String,
                page_id: Long,
                action_time: String,
                search_keyword: String,
                click_category_id: Long,
                click_product_id: Long,
                order_category_ids: String,
                order_product_ids: String,
                pay_category_ids: String,
                pay_product_ids: String,
                city_id: Long
                ) if (order_category_ids != "null") => {
                    // 这个下单可能会出现，一次下单很多商品的情况
                    //order_category_ids.flatMap(x => List(CategoryCountInfo(x.toString,0,1,0)))
                    val ids: Array[String] = order_category_ids.split(",")
                    val list: ListBuffer[CategoryCountInfo] = new ListBuffer[CategoryCountInfo] // 这里使用ListBuffer是因为需要一个可变的集合
                    ids.foreach(id => list.append(CategoryCountInfo(id, 0, 1, 0)))
                    list
                }

                case UserVisitAction(
                date: String,
                user_id: Long,
                session_id: String,
                page_id: Long,
                action_time: String,
                search_keyword: String,
                click_category_id: Long,
                click_product_id: Long,
                order_category_ids: String,
                order_product_ids: String,
                pay_category_ids: String,
                pay_product_ids: String,
                city_id: Long
                ) if (pay_category_ids != "null") => {
                    val ids: Array[String] = pay_category_ids.split(",") // 这个支付可能会出现，一次支付很多商品的情况
                    val list: ListBuffer[CategoryCountInfo] = new ListBuffer[CategoryCountInfo] // 这里使用ListBuffer是因为需要一个可变的集合
                    ids.foreach(id => list.append(CategoryCountInfo(id, 0, 0, 1)))
                    list
                }

                case _ => Nil; // 默认匹配找不到就是空, 或者说不满足条件的就是直接返回空
            }
        })

        // TODO 5：根据相同品类数据进行分组
        val groupRDD: RDD[(String, Iterable[CategoryCountInfo])] = flatMapRDD.groupBy(info => info.categoryId)

        // TODO 6：将分组后的数据进行聚合, 应该是相同品类的相同类型的数据进行聚合
        /* 这样写返回的类型中，第一个是品类id，后面的值也有，重复了，所以可以直接使用map修改结构
        groupRDD.mapValues( datas => {
            datas.reduce( (x,y) => {                            // x , y 其实就是CategoryCountInfo对象
                x.clickCount = x.clickCount + y.clickCount      // 样例类中的属性是不能修改的，想要修改需要在样例类中使用var进行声明（默认是val）
                x.orderCount = x.orderCount + y.orderCount
                x.payCount = x.payCount + y.payCount
            })
        })*/

        /*这里有一个问题： reduce的datas是map算子，map算子里面的参数是一个集合，集合的数据量如果非常大可能会内存溢出
        * 所以可以使用累计器，进行累加数据，而且累加器是分布式的*/
        val infoRDD: RDD[CategoryCountInfo] = groupRDD.map {
            case (cid, datas) => {
                // 集合内部数据聚合使用reduce， 集合外部数据聚合使用fold
                datas.reduce((x, y) => { // x , y 其实就是CategoryCountInfo对象
                    x.clickCount = x.clickCount + y.clickCount // 样例类中的属性是不能修改的，想要修改需要在样例类中使用var进行声明（默认是val）
                    x.orderCount = x.orderCount + y.orderCount
                    x.payCount = x.payCount + y.payCount
                    x
                })
            }
        }

        // TODO 7：排序,降序 （点击，下单，支付）
        // sortBy的第一个参数是要将需要排序的对象变成可以排序的东西，这里无法转成数字，字符串也不行，可以考虑使用元祖 （100,50,20）
        // tuple类型本身就有一个顺序排序的概念，先比较第一个，相同继续比较第二个，以此类推
        val result: Array[CategoryCountInfo] = infoRDD.sortBy(info => (info.clickCount, info.orderCount, info.payCount), false).take(10)

        // TODO 8： 打印每个样例类
        result.foreach(println(_))

        // TODO  最后一步： 释放链接
        sc.stop()
    }
}
