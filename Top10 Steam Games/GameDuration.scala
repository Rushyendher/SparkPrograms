package com.rushyendher.steam

import org.apache.spark._
import org.apache.spark.SparkContext._

object GameDuration {
    
    def parseFile(line: String) = {
        val fields = line.split(",")
        val game = fields(1)
        val duration = if(fields.length == 5) fields(3) else "0"
        (game, duration.toDouble)
    }
  
    def main(args: Array[String]) {
        val sc = new SparkContext("local[*]", "SteamGameDuration")
        
        val lines = sc.textFile("../steam-games.csv")
        val filterPlayTime = lines.filter(_.contains(",play,"))
        val parsedLines = filterPlayTime.map(parseFile)
        val gameDuration = parsedLines.reduceByKey((a,b) => a+b)

        val topTen = gameDuration.sortBy(_._2, ascending = false).take(10)
        topTen.foreach(println)
    }
  
}