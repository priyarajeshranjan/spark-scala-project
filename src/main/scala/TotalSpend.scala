
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object TotalSpend extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","totalspend")
  val input = sc.textFile("/Users/rajesh/Downloads/trendy_tech_trainning/week9spark/customerorders.csv")
  val mappedInput = input.map(x => (x.split(",")(0),x.split(",")(2).toFloat))
  val totalByCostomer = mappedInput.reduceByKey((x,y)=>x+y)
  val sortedTotal = totalByCostomer.sortBy(x=> x._2)
  val result = sortedTotal.collect()
  result.foreach(println)



}
