package com.gendlee.main

import com.gendlee.graph.GraphX
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Application {
    Logger.getLogger("org").setLevel(Level.ERROR)

    def main(args: Array[String]): Unit = {
        println("Application is running")
        val conf = new SparkConf().setAppName("sparkdemo").setMaster("local[*]")
        val sc = new SparkContext(conf)

        try {
            GraphX.run(sc)
        } catch {
            case e: Exception => println("Application.main Exception: " + e.getStackTrace)
        }


    }
}