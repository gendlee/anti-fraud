import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object testMain {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    println("testMain is running")
    val conf = new SparkConf().setAppName("sparkdemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val input = sc.textFile("pom.xml")
    val lines = input.flatMap(x => x.split("\\s"))
    val count = lines.map(x => (x, 1)).reduceByKey(_+_)
      .collect()

    count.foreach(println)

  }
}