import org.apache.spark.sql.functions.col

// Sample Data
val data = Seq(
  (1, "USA", "California", "Los Angeles", "West", "Sales", "Executive", "John"),
  (2, "USA", "USA", "USA", "USA", "IT", "Engineer", "Ahmad"),
  (3, "India", "Karnataka", "Bangalore", "South", "Data & AI", "Data Engineer", "Ramesh"),
  (4, "Germany", "Germany", "Germany", "Germany", "HR", "Associate", "Albert"),
  (5, "USA", "Texas", "Houston", "South", "Sales", "Sales Man", "Linda"),
  (6, "India", "Maharashtra", "Mumbai", "West", "Finance", "Manager", "Hasan")
)

val df = data.toDF("ID", "Country", "State", "City", "Zone", "Department", "Role", "Manager")

//APPROACH-1
val filterCondition1 = (
  (col("Country") === col("State")) and 
  (col("State") === col("City")) and
  (col("City") === col("Zone"))
)

val resultDF = (
  df.filter(filterCondition1.unary_!)
)


//APPROACH-2
val columns = List(col("Country"), col("State"), col("City"), col("Zone"))

val filterCondition2 = columns
  .sliding(2)    
  .map {
    case List(leftColumn, rightColumn) => leftColumn === rightColumn
  }
  .reduce((leftCondition, rightCondition) => leftCondition and rightCondition)

val resultDf = df.filter(filterCondition2.unary_!)

