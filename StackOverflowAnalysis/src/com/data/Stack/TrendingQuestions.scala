package com.data.Stack

import org.apache.spark.sql.SparkSession
import scala.xml.XML

import java.lang.Integer

object TrendingQuestions {

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
			 ( Integer.parseInt(xml.attribute("Score").getOrElse(0).toString()),line)}
      }
      .sortByKey(false)
     .take(10)
			   
       
			spark.sparkContext.parallelize(result.toSeq,1).saveAsTextFile(args(1))
	spark.stop
  }
}