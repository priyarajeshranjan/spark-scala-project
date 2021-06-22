
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.io.Source

object LogLevel extends App {


  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","keywordamount")

  val myList = List ("WARN : Running task 1.0 in stage 0.0 (TID 1)",
            "INFO : Running task 0.0 in stage 0.0 (TID 0)",
            "INFO : running: Set()",
            "ERROR : waiting: Set(ResultStage 1)",
            "ERROR : failed: Set()",
            "INFO : Running task 0.0 in stage 3.0 (TID 4)",
            "INFO : Running task 1.0 in stage 3.0 (TID 5)")

  val originalLogRdd = sc.parallelize(myList)
  val newPairRdd = originalLogRdd.map(x => {
    val column = x.split(":")
    val logLevel = column(0)
    (logLevel,1)

  })
  val result = newPairRdd.reduceByKey((x,y) => x+y)
  result.collect()foreach(println)
}
