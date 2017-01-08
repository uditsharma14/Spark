import org.apache.spark.SparkContext._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object Maxwages{
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Max Wages")
    val sc = new SparkContext(conf)
    sc.textFile(args(0))
      .map(_.split("\t"))
      .map(rec => (rec(0), rec(2).toInt))
      .reduceByKey((a, b) => Math.max(a, b))
      .saveAsTextFile(args(1))
  }
}
