// Databricks notebook source
import spark.implicits._

// COMMAND ----------

val data = Seq(
    (101, "Mohsin", "Alam"),
    (102, "Parth", "Nigam"),
    (103, "Suraz", "Kumar"),
    (104, "Shubham", "Kumar"),
    (105, "Ahmad", "Hasan")
)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Postive lookahead assertion
// MAGIC I will use a special type of Regular Expression known as <i>postive lookahead assertion</i>  
// MAGIC <b>"(?=...)".r :</b> This is a positive lookahead assertion. It's a zero-width assertion, meaning it doesn't consume any characters in the string. It asserts that what immediately follows the current position in the string is the pattern inside the lookahead.  
// MAGIC We will use <b>[A-Z]</b> to match the upper case character in the column names. SO our final regeEx pattern will be <b>"(?=[A-Z])"</b>   
// MAGIC Now let's take some example to show how this pattern will work
// MAGIC ```scala
// MAGIC scala> val regPattern = "(?=[A-Z])".r
// MAGIC val regPattern: scala.util.matching.Regex = (?=[A-Z])
// MAGIC scala> val example1 = "FirstName"
// MAGIC val example1: String = FirstName
// MAGIC scala> val example2 = "ProductSubCategory"
// MAGIC val example2: String = ProductSubCategory
// MAGIC scala> val splitEx1 = regPattern.split(example1)
// MAGIC val splitEx1: Array[String] = Array(First, Name)
// MAGIC scala> val splitEx2 = regPattern.split(example2)
// MAGIC val splitEx2: Array[String] = Array(Product, Sub, Category)
// MAGIC ```

// COMMAND ----------

val df = data.toDF("Id","FirstName","LastName")
val regExpattern = "(?=[A-Z])".r  //postive lookahead assertion  type of regular expression
val renamedColumns = for{
  col <- df.columns
}yield{
  val splitColumnName = regExpattern.split(col)   
  if (splitColumnName.length > 1) splitColumnName.mkString("_")
  else splitColumnName(0)
}

val renamedDF = df.toDF(renamedColumns : _*)  //unpacking the array to pass each element as an argument to toDF
renamedDF.show

// COMMAND ----------


