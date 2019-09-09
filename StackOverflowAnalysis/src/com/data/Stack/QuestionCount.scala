package com.data.Stack

import org.apache.spark.sql.SparkSession
import java.lang.Long

object QuestionCount {

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: QuestionCount <Input-File> <Output-File>");
      System.exit(1);
    }
      val spark = SparkSession
				.builder
				.appName("MostPopulous")
				.getOrCreate()
				
			val data = spark.read.textFile(args(0)).rdd
			
			val result = data.filter { line => {line.trim().startsWith("<row")}
      }.filter {line => {line.contains("PostTypeId=\"1\"")}
			}
			
       
    result.saveAsTextFile(args(1))
			spark.stop
  }
}