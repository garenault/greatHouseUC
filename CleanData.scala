// Databricks notebook source
// MAGIC %run "./Get DFs"

// COMMAND ----------

// MAGIC %md
// MAGIC ###Clean District DFs

// COMMAND ----------

val districtDF = ghhDistrictDF.unionAll(rbDistrictDF).distinct

// COMMAND ----------

import org.apache.spark.sql.functions.{col,regexp_replace}
import org.apache.spark.sql.types.DoubleType

val stockDF = ghhStockDF.unionAll(rbStockDF).distinct

val stockDFPrice = (stockDF
          .withColumn("price", regexp_replace(col("price"), ",", "."))
          .withColumn("price", col("price").cast(DoubleType))
          )

// COMMAND ----------

districtDF.show(5)

// COMMAND ----------

stockDFPrice.show(5)

// COMMAND ----------

val fileName = "/mnt/greathouse/silver/district.parquet"
print("Output location: " + fileName)

(districtDF.write                  // Our DataFrameWriter
  .option("compression", "snappy") // One of none, snappy, gzip, and lzo
  .mode("overwrite")               // Replace existing files
  .parquet(fileName)               // Write DataFrame to Parquet files
)

// COMMAND ----------

val fileName = "/mnt/greathouse/silver/stock.parquet"
print("Output location: " + fileName)

(stockDFPrice.write                  // Our DataFrameWriter
  .option("compression", "snappy") // One of none, snappy, gzip, and lzo
  .mode("overwrite")               // Replace existing files
  .parquet(fileName)               // Write DataFrame to Parquet files
)
