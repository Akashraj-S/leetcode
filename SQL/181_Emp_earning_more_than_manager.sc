
import org.apache.spark.sql.SparkSession


val spark = SparkSession.builder()
  .appName("BankingDataIngestion")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._

val df = Seq(
  (1, "Joe",   70000, Some(3)),
  (2, "Henry", 80000, Some(4)),
  (3, "Sam",   60000, None),
  (4, "Max",   90000, None)
).toDF("id", "name", "salary", "managerId")

df.show()


/*

Key points (important for interviews)
alias("a"), alias("b") → required for self join
=== instead of == → Spark column comparison (you already asked this earlier 👍)
Join condition combines:
a.managerId = b.id
a.salary > b.salary
.select(col("a.name").as("Employee")) → same as SQL alias
 */