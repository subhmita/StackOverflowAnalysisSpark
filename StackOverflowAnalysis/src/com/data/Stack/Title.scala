package com.data.Stack

import org.apache.spark.sql.SparkSession
import scala.xml.XML

import java.lang.Long

object Title {

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: Title <Input-File> <Output-File>");
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
      .flatMap {line => {
			  val xml = XML.loadString(line)
			  xml.attribute("Title")}
      }
      .filter{line => {line.mkString.toLowerCase.contains("hadoop") ||line.mkString.toLowerCase.contains("nosql") }
      }
			
       
   val count = result.count    
       
	spark.sparkContext.parallelize(Seq(count),1).saveAsTextFile(args(1))		
	spark.stop
  }
}