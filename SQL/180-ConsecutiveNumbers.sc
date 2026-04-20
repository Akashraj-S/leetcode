
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val spark = SparkSession.builder()
  .appName("BankingDataIngestion")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._

val data = Seq(
  (1, 1),
  (2, 1),
  (3, 1),
  (4, 2),
  (5, 1),
  (6, 2),
  (7, 2),
  (8, 1),
  (9, 1),
  (10, 1)
)

// Convert to DataFrame
var df = data.toDF("id", "num")



val windowspec = Window.orderBy("id")

df = df.withColumn("a",lag("num",0).over(windowspec))
df = df.withColumn("b",lag("num",1).over(windowspec))
df = df.withColumn("c",lag("num",2).over(windowspec))

df = df.withColumnRenamed("num","ConsecutiveNums")
  .filter(col("a")===col("b")&&col("b")===col("c"))
df.select("ConsecutiveNums").distinct().show()


/* Question
Find all numbers that appear at least three times consecutively.

Return the result table in any order.

The result format is in the following example.


Example 1:

Input:
Logs table:
+----+-----+
| id | num |
+----+-----+
| 1  | 1   |
| 2  | 1   |
| 3  | 1   |
| 4  | 2   |
| 5  | 1   |
| 6  | 2   |
| 7  | 2   |
+----+-----+
Output:
+-----------------+
| ConsecutiveNums |
+-----------------+
| 1               |
+-----------------+
Explanation: 1 is the only number that appears consecutively for at least three times.


 */