package com.xl.streaming

import java.sql.DriverManager
import java.util

import com.xl.streaming.bean.clickCountsBean

object JDBCUtils {
  //批量更新mysql数据
  def updateBatch(adUserClickCounts: util.ArrayList[clickCountsBean]): Unit ={
    import scala.collection.JavaConversions._

    Class.forName("com.mysql.jdbc.Driver")
    val connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall", "root", "000000")
    connection.setAutoCommit(false)

    // 首先对用户广告点击量进行分类，分成待插入的和待更新的
    val insertAdUserClickCounts = new util.ArrayList[clickCountsBean]
    val updateAdUserClickCounts = new util.ArrayList[clickCountsBean]

    for(clickCount:clickCountsBean <- adUserClickCounts){
      val stmt = connection.createStatement()
      val selectSQL = "select count(*) from ad_user_click_count where date='" + clickCount.date + "' " +
        "and userId='" + clickCount.userId + "' and adId='" + clickCount.adId + "'"
      val result = stmt.executeQuery(selectSQL)
      result.next()
      val count = result.getInt(1)

      if(count == 1){
        //数据库中有数据，做更新操作,将要更新的数据放入updateAdUserClickCounts
        updateAdUserClickCounts.add(clickCount)
      }else{
        //数据库中没有数据，做插入操作，将要插入的数据放入insertAdUserClickCounts
        insertAdUserClickCounts.add(clickCount)
      }
    }
    //批量插入
    val pstmt = connection.prepareStatement("insert into ad_user_click_count values(?,?,?,?)")
    for(clickCount:clickCountsBean <- insertAdUserClickCounts){
      pstmt.setString(1, clickCount.date)
      pstmt.setString(2, clickCount.userId)
      pstmt.setString(3, clickCount.adId)
      pstmt.setLong(4, clickCount.clickCount)
      pstmt.addBatch()
    }
    pstmt.executeBatch()
    connection.commit()

    //批量更新
    val pstmt2 = connection.prepareStatement("update ad_user_click_count set clickCount=clickCount+? where date=? and userId=? and adId=?")
    for(clickCount:clickCountsBean <- updateAdUserClickCounts){
      pstmt2.setLong(1, clickCount.clickCount)
      pstmt2.setString(2, clickCount.date)
      pstmt2.setString(3, clickCount.userId)
      pstmt2.setString(4, clickCount.adId)
      pstmt2.addBatch()
    }
    pstmt2.executeBatch()

    connection.commit()
    connection.close()
  }

  //添加数据到黑名单中
  def addBlackUser(adBlacklists: util.ArrayList[String]): Unit ={
    import scala.collection.JavaConversions._

    Class.forName("com.mysql.jdbc.Driver")
    val connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall", "root", "000000")
    connection.setAutoCommit(false)

    //批量插入
    val pstmt = connection.prepareStatement("insert into ad_blacklist values(?)")
    for(userId:String <- adBlacklists){
      pstmt.setString(1, userId)
      pstmt.addBatch()
    }
    pstmt.executeBatch()
    connection.commit()
    connection.close()
  }

  //查询某天某人点击某广告次数
  def findClickCount(date: String, userId: String, adId: String): Long ={
    Class.forName("com.mysql.jdbc.Driver")
    val connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall", "root", "000000")

    val stmt = connection.createStatement()
    val selectSQL = "select clickCount from ad_user_click_count where date='" + date + "' " +
      "and userId='" + userId + "' and adId='" + adId + "'"
    val result = stmt.executeQuery(selectSQL)
    result.next()
    val count = result.getLong(1)
    connection.close()
    count
  }

  //查询出黑名单中所有userId
  def findBlackUserId(): util.ArrayList[String] ={
    Class.forName("com.mysql.jdbc.Driver")
    val connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall", "root", "000000")

    val blackUserId = new util.ArrayList[String]()

    val stmt = connection.createStatement()
    val selectSQL = "select userId from ad_blacklist"
    val result = stmt.executeQuery(selectSQL)
    while(result.next()){
      val userId = result.getString(1)
      blackUserId.add(userId)
    }

    connection.close()
    blackUserId
  }

}
