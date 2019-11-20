package com.xl.streaming

import java.util

import com.xl.streaming.bean.clickCountsBean
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}

object AdClickRealTimeStatSpark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("AdClickRealTimeSpark")

    val ssc = new StreamingContext(conf, Durations.seconds(5))

    val spark = SparkSession.builder().getOrCreate()

    //updateStateByKey算子计算的中间数据需要保存在一个目录中
    ssc.checkpoint(".")

    val kafkaParams = Map[String, String]("metadata.broker.list" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092")

    val topics = Set("AdRealTimeLogs")

    // kafka数据格式：某个时间点 某个省份 某个城市 某个用户 某个广告
    val adRealTimeLogDStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topics)

    // 根据动态黑名单进行数据过滤
    val filteredAdRealTimeLogDStream = filterByBlacklist(adRealTimeLogDStream)

    // 生成动态黑名单
    generateDynamicBlacklist(filteredAdRealTimeLogDStream)

    //业务功能1：计算广告点击流量实时统计结果（yyyyMMdd_province_city_adid,clickCount）
    val adRealTimeStatDStream = calculateRealTimeStat(filteredAdRealTimeLogDStream)

    // 业务功能2：实时统计每天每个省份top3热门广告
    calculateProvinceTop3Ad(spark, adRealTimeStatDStream)

    //业务功能3：实时统计每天每个广告在最近1小时的滑动窗口内的点击趋势（每分钟的点击量）
    calculateAdClickCountByWindow(adRealTimeLogDStream)

    ssc.start()
    ssc.awaitTermination()
  }

  def filterByBlacklist(adRealTimeLogDStream: InputDStream[(String, String)]): DStream[(String, String)] = {
    import scala.collection.JavaConversions._

    val filterDStream = adRealTimeLogDStream.transform(adRealTimeLogRDD => {
      val sc = adRealTimeLogRDD.sparkContext

      //查询出黑名单用户
      val blackUserId = JDBCUtils.findBlackUserId()
      //序列化黑名单用户
      val userIdRDD = sc.parallelize(blackUserId)
      /*
      * 将adRealTimeLogRDD和userIdRDD转换格式以便join过滤
      * 将adRealTimeLogRDD转换成[userId，<String,String>]格式
      * 将userIdRDD转换成[userId，true]格式
      * */
      val userId_adRealTimeLogRDD = adRealTimeLogRDD.map(tuple =>(tuple._2.split("")(1),tuple))
      val userIdBoolean = userIdRDD.map((_, true))
      //左外连接进行黑名单用户匹配
      val joinedRDD = userId_adRealTimeLogRDD.leftOuterJoin(userIdBoolean)
      //过滤掉匹配上的黑名单的数据
      val filterjoinedRDD = joinedRDD.filter(tuple => tuple._2._2==None)
      val rdd = filterjoinedRDD.map(_._2._1)
      rdd
    })
    filterDStream
  }

  def generateDynamicBlacklist(filteredAdRealTimeLogDStream: DStream[(String, String)]) = {
    //将日志的格式处理成<yyyyMMdd_userid_adid, 1L>格式
    val dailyUserAdClickDStream = filteredAdRealTimeLogDStream.map(_._2.split(" ")).map(x => (x(0)+"_"+x(3)+"_"+x(4),1L))

    // <yyyyMMdd_userid_adid, clickCount>
    val dailyUserAdClickCountDStream = dailyUserAdClickDStream.reduceByKey(_+_)

    //批量插入数据
    dailyUserAdClickCountDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iterator => {
        val adUserClickCounts = new util.ArrayList[clickCountsBean]()

        for(userClickCount <- iterator){
          val date = userClickCount._1.split("_")(0).substring(0,8)
          val userId = userClickCount._1.split("_")(1)
          val adId = userClickCount._1.split("_")(2)
          val clickCount = userClickCount._2
          val adUserClickCount = new clickCountsBean(date, userId, adId, clickCount)

          adUserClickCounts.add(adUserClickCount)
        }

        JDBCUtils.updateBatch(adUserClickCounts)
      })
    })

    //点击次数超过5次就过滤到黑名单
    val blackListDStream = dailyUserAdClickDStream.filter(dailyDStream => {
      val key = dailyDStream._1
      val date = key.split("_")(0).substring(0,8)
      val userId = key.split("_")(1)
      val adId = key.split("_")(2)

      val clickCount = JDBCUtils.findClickCount(date, userId, adId)

      clickCount>=5
    })

    //取出userId
    val blacklistUseridDStream = blackListDStream.map(_._1.split("_")(1))
    //对userId去重
    val distinctBlacklistUseridDStream = blacklistUseridDStream.transform(_.distinct())

    //将黑名单保存到MySQL中
    distinctBlacklistUseridDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iterator => {
        val adBlacklists = new util.ArrayList[String]()
        for(userId <- iterator){
          adBlacklists.add(userId)
        }

        JDBCUtils.addBlackUser(adBlacklists)
      })
    })
  }

  def calculateRealTimeStat(filteredAdRealTimeLogDStream: DStream[(String, String)]) = {
    //数据格式：日期、省份、城市、广告，点击次数
    val mappedDStream = filteredAdRealTimeLogDStream.map(_._2.split(" ")).map(x => ((x(0).substring(0,8),x(1),x(2),x(4)),1))
    //利用updateStateByKey保存一份数据在spark中
    val aggregatedDStream = mappedDStream.updateStateByKey((values: Seq[Int], state: Option[Int]) =>{
      val currentCount = values.sum
      val count = state.getOrElse(0)
      Some(currentCount + count)
    })

    //想看效果的可以打印出来在控制台上看效果
    //aggregatedDStream.print()

    /*
     *   TODO
     *   将aggregatedDStream写入mysql,这里仿照前面批量插入和更新数据到mysql即可
     */

    aggregatedDStream
  }

  def calculateProvinceTop3Ad(spark: SparkSession, adRealTimeStatDStream: DStream[((String, String, String, String), Int)]) = {
    //隐式转换
    import spark.implicits._

    //在transform中操作rdd
    val rowsDStream = adRealTimeStatDStream.transform(rdd => {
      val mappedRDD = rdd.map(rdd => ((rdd._1._1, rdd._1._2, rdd._1._4), 1))
      val dailyAdClickCountByProvinceRDD = mappedRDD.reduceByKey(_+_)
      val rowRDD = dailyAdClickCountByProvinceRDD.map(r =>Row(r._1._1,r._1._2,r._1._3,r._2))

      //转换为dateframe格式，写sql取top3
      val schema = StructType(List(
        StructField("date", StringType),
        StructField("province", StringType),
        StructField("adId", StringType),
        StructField("clickCount", IntegerType)
      ))

      val df = spark.createDataFrame(rowRDD, schema)
      df.createOrReplaceTempView("tmp_daily_ad_click_count_by_prov")
      val result = spark.sql("" +
        "select * from (" +
        "   select date, province, adId, clickCount, " +
        "   row_number() over(partition by date,province order by clickCount desc) rank " +
        "   from tmp_daily_ad_click_count_by_prov" +
        ") a where a.rank<=3")

      result.rdd
    })

    //想看效果的可以打印出来在控制台上看效果
//    rowsDStream.print()

    /*
     *   TODO
     *   将rowsDStream写入mysql,这里仿照前面批量插入和更新数据到mysql即可
     */
  }

  def calculateAdClickCountByWindow(adRealTimeLogDStream: InputDStream[(String, String)]) = {
    //<yyyyMMddHHmm_adid,1>格式
    val mappedDStream = adRealTimeLogDStream.map(_._2.split(" ")).map(x => (x(0)+"_"+x(3),1))
    val aggrDStream = mappedDStream.reduceByKeyAndWindow((a:Int, b:Int) => a+b, Durations.minutes(60), Durations.seconds(10))

    aggrDStream.print()

    /*
     *   TODO
     *   将aggrDStream写入mysql,这里仿照前面批量插入和更新数据到mysql即可
     */
  }
}
