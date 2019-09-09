package com.data.Stack

import org.apache.spark.sql.SparkSession
import java.lang.Long
import java.text.SimpleDateFormat
import java.util.Date
import scala.xml.XML
import java.util.Calendar

object ActiveQuestion {

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: MonthlyQuestionCount <Input-File> <Output-File>");
      System.exit(1);
    }
    // CreationDate="2014-05-13T23:58:30.457" 
    val format1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");   
   
    val spark = SparkSession
				.builder
				.appName("MostPopulous")
				.getOrCreate()
				
			val data = spark.read.textFile(args(0)).rdd
			
			val result = data.filter { line => {line.trim().startsWith("<row")}
      }
    .filter {line => {line.contains("PostTypeId=\"1\"")}
      }		
			.map { line =>{
			   val xml = XML.loadString(line)
			 ( xml.attribute("CreationDate").get,xml.attribute("LastActivityDate").get,line)
			}
			}.map {data =>
			  val crDate = format1.parse(data._1.text)
			  val crTime = crDate.getTime
			  
			   val enDate = format1.parse(data._2.text)
			  val enTime = enDate.getTime
			  
			  val timeDiff :Long = enTime -crTime
			  (crTime,enTime,timeDiff,data._3)
			}
       .filter {data => {data._3/(1000*60*60*24)>30*6}       
       
       }
    result.saveAsTextFile(args(1))
			spark.stop
  }
}