package com.data.Stack

import org.apache.spark.sql.SparkSession
import java.lang.Long
import java.text.SimpleDateFormat
import java.util.Date
import scala.xml.XML
import java.util.Calendar

object QuestionwithTagAndTimeRange {

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: MonthlyQuestionCount <Input-File> <Output-File>");
      System.exit(1);
    }
    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
      val format2 = new SimpleDateFormat("yyyy-MM");
      val format3 = new SimpleDateFormat("yyyy-MM-dd");
      
      val startTime = format3.parse("2015-01-01").getTime
      val endTime = format3.parse("2015-01-31").getTime
      
			val spark = SparkSession
				.builder
				.appName("AvgAnsTime")
				.master("local")
				.getOrCreate()
				
			//Read some example file to a test RDD
			val data = spark.read.textFile(args(0)).rdd


			val result = data.filter{line => {line.trim().startsWith("<row")}
			}
			.filter { line => {line.contains("PostTypeId=\"1\"")}
			}
			.map {line => {
			  val xml = XML.loadString(line)
			  val crDate = xml.attribute("CreationDate").get.toString()
			  val tags = xml.attribute("Tags").get.toString()
//			  (closeDate, line)
			  (crDate, tags, line)
			  }
			}
			.filter{ data => {
			  var flag = false
			  val crTime = format.parse(data._1.toString()).getTime
			  if (crTime > startTime && crTime < endTime && (data._2.toLowerCase().contains("bigdata") || 
			      data._2.toLowerCase().contains("hadoop") || data._2.toLowerCase().contains("spark")))
			    flag = true
			  flag
			  }
			}
		
			result.foreach { println }
			println(result.count())
			
			spark.stop
	}

   }