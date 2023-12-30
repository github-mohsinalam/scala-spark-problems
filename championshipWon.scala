
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object championshipWon extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("demo")
    .getOrCreate()

  import spark.implicits._
  val playersDF = List((1, "Nadal"), (2, "Federer"), (3, "Novak")).toDF("playerId","player")
  val championshipsDF = List((2018, 1, 1, 1, 1), (2019, 1, 1, 2, 2), (2020, 2, 1, 2, 2)).toDF("year","Wimbledon","French","AU","US")
  val championshipUnpviotDF = championshipsDF.unpivot(Array(col("year")),"Championship","playerId")
  championshipUnpviotDF.select("playerId")
    .groupBy("playerId")
    .count()
    .join(playersDF,"playerId")
    .select(
      col("playerId"),
      col("player"),
      col("count").as("grandSlamCount")
    )
    .show
}
