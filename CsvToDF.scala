package iplcricket

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext._
import org.apache.log4j._

object MatchCount {
  
  def main(args: Array[String]) {
        Logger.getLogger("org").setLevel(Level.ERROR)
        val sc = new SparkContext("local[*]", "MatchCount")
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        val df = sqlContext.read.format("csv").option("header","true").option("infraSchema","true").load("/Users/bhargav/Desktop/ScalaCourse/csvfiles/IPLdata.csv")
        df.registerTempTable("CricketData")
        sqlContext.sql("select match_winner, count(*) from CricketData Group By match_winner").show()
  }
  
}

object TossWinner {
  
  def main(args: Array[String]) {
        Logger.getLogger("org").setLevel(Level.ERROR)
        val sc = new SparkContext("local[*]", "MatchCount")
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        val df = sqlContext.read.format("csv").option("header","true").option("infraSchema","true").load("/Users/bhargav/Desktop/ScalaCourse/csvfiles/IPLdata.csv")
        df.registerTempTable("CricketData")
        sqlContext.sql("select count(*) as TossandMatch from CricketData where Toss_Winner = match_winner").show()
  }
}
