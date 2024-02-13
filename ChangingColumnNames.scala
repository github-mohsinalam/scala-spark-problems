package practice

import org.apache.spark.sql.SparkSession

object ChangingColumnNames extends App{
  val spark = SparkSession.builder()
    .appName("Changing Column Names")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  val data = Seq(
    (101, "Mohsin", "Alam"),
    (102, "Parth", "Nigam"),
    (103, "Suraz", "Kumar"),
    (104, "Shubham", "Kumar"),
    (105, "Ahmad", "Hasan")
  )

  /**
   *  I will use a special type of Regular Expression known as <i>postive lookahead assertion</i> .<br>
<b>"(?=...)".r :</b> This is a positive lookahead assertion. It's a zero-width assertion, meaning it doesn't consume any characters in the string. It asserts that what immediately follows the current position in the string is the pattern inside the lookahead.
We will use <b>[A-Z]</b> to match the upper case character in the column names. SO our final regeEx pattern will be <b>"(?=[A-Z])"</b>
Now let's take some example to show how this pattern will work <br>
   *  @example
      scala> val regPattern = "(?=[A-Z])".r <br>
      val regPattern: scala.util.matching.Regex = (?=[A-Z]) <br>
      scala> val example1 = "FirstName" <br>
      val example1: String = FirstName <br>
      scala> val example2 = "ProductSubCategory" <br>
      val example2: String = ProductSubCategory <br>
      scala> val splitEx1 = regPattern.split(example1) <br>
      val splitEx1: Array[String] = Array(First, Name) <br>
      scala> val splitEx2 = regPattern.split(example2) <br>
      val splitEx2: Array[String] = Array(Product, Sub, Category) <br>

   */

  val df = data.toDF("Id","FirstName","LastName")
  val regPattern = "(?=[A-Z])".r  //positive lookahead assertion
  val renamedColumns: Array[String] = for {
    col <- df.columns
  } yield {
    val splitName = regPattern.split(col)
    if (splitName.length > 1) splitName.mkString("_")
    else splitName(0)
  }

  val renamedDF = df.toDF(renamedColumns : _*)
  renamedDF.show()


}
