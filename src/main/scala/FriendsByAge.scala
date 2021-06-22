
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object FriendsByAge extends App {

  def parseLine (line : String )={
    val fields = line.split("::")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    (age,numFriends)
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","friendsbyage")
  val input = sc.textFile("/Users/rajesh/Downloads/trendy_tech_trainning/week9spark/friendsdata.csv")

  val mappedInput = input.map(parseLine)
  val mappedFinal = mappedInput.mapValues(x => (x,1))
  val totalsByAge = mappedFinal.reduceByKey((x,y) => (x._1 + y._1 , x._2 + y._2))
  val averageByAge = totalsByAge.mapValues(x =>  x._1/x._2).sortBy(x => x._2)
  averageByAge.collect.foreach(println)



}
