/*
various approaches to create conditional column in a DataFrame
*/

import org.apache.spark.sql.functions._

val data = Seq("One","Two","Three","Four","Five")
val df = data.toDF("Word")

//METHOD1  - Leveraging string interpolation 

val condition = Seq(("One",1),("Two",2),("Three",3),("Four",4),("Five",5))

val logicString = s"""CASE ${condition.map{case(k,v) => s"WHEN Word = '$k' THEN $v"}.mkString(" ")} ELSE NULL END"""

val newDf = df.select(
  col("Word"),
  expr(logicString).as("Numeric")
)

//METHOD2  - Manual case statement

val caseString = """
CASE 
  WHEN Word = 'One' THEN 1 
  WHEN Word = 'Two' THEN 2 
  WHEN Word = 'Three' THEN 3 
  WHEN Word = 'Four' THEN 4 
  WHEN Word = 'Five' THEN 5
  ELSE NULL
END
"""

val newDf1 = df.select(
  col("Word"),
  expr(caseString).as("Numeric")
)


//METHOD3 - Using when and otherwise

val newDf2 = df.select(
  col("Word"),
  when(col("Word") === "One",1)
    .when(col("Word") === "Two",2)
    .when(col("Word") === "Three",3)
    .when(col("Word") === "Four",4)
    .when(col("Word") === "Five",5)
    .otherwise(null)
  .as("Numeric")
)
