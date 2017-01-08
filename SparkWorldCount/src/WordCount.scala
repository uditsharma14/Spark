import org.apache.spark.SparkContext._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object WordCount{
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark WordCount")
    val sc = new SparkContext(conf)
    if (args.length < 2) {
      println("Usage: ScalaWordCount <input> <output>")
      System.exit(1)
    }
    // reading the input file and create RDD
    var rawData= sc.textFile(args(0));
    
    // converting the line into the words using flat map
    val words = rawData.flatMap(line => line.split(" "))
    
    //count the individual words using map and reduceByKey operation 
    val wordCount = words.map(eachWord => (eachWord, 1)).reduceByKey(_ + _)
    
    wordCount.saveAsTextFile(args(1));
  }
}