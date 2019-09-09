package com.data.Stack

import org.apache.spark.sql.SparkSession
import java.lang.Long
import java.text.SimpleDateFormat
import java.util.Date
import scala.xml.XML

object MonthlyQuestionCount {

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: MonthlyQuestionCount <Input-File> <Output-File>");
      System.exit(1);
    }
    // CreationDate="2014-05-13T23:58:30.457" 
    val format1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");   
    val format2 = new SimpleDateFormat("yyyy-MM");
    
    val spark = SparkSession
				.builder
				.appName("MostPopulous")
				.getOrCreate()
				
			val data = spark.read.textFile(args(0)).rdd
			
			val result = data.filter { line => {line.trim().startsWith("<row")}
      }
    .filter {line => {line.contains("PostTypeId=\"1\"")}
      }
			.flatMap {line => {
			  val xml = XML.loadString(line)
			  xml.attribute("CreationDate")
			}
			}
			.map { line =>{
			  (format2.format(format1.parse(line.toString())),1)
			}
			}.reduceByKey(_+_)
       
    result.saveAsTextFile(args(1))
			spark.stop
  }
}