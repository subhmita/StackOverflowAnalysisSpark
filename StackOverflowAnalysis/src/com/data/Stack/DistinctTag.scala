package com.data.Stack

import org.apache.spark.sql.SparkSession
import scala.xml.XML

import java.lang.Integer

object DistinctTag {

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: TrendingQuestions <Input-File> <Output-File>");
      System.exit(1);
    }
      val spark = SparkSession
				.builder
				.appName("Title")
				.getOrCreate()
				
			val data = spark.read.textFile(args(0)).rdd
			
			val result = data.filter { line => {line.trim().startsWith("<row")}
      }
      .filter {line => {line.contains("PostTypeId=\"1\"")}
			}
      .map {line => {
			  val xml = XML.loadString(line)
			 ( xml.attribute("Tags").get.toString)}
      }.
      flatMap {data => {
        data.replaceAll("&lt;", " ").replaceAll("&gt;", " ").split(" ")
      }
      
      }.filter{tag => {tag.length>0}}
      .map{ data => {
        (data,1)
      }
      }
			.reduceByKey(_+_)  	
			.sortByKey(true)
    result.saveAsTextFile(args(1))
	  spark.stop
  }
}