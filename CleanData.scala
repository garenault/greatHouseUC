// Databricks notebook source
// MAGIC %run "./Get DFs"

// COMMAND ----------

// MAGIC %md
// MAGIC ###Clean District DFs

// COMMAND ----------

val districtDF = ghhDistrictDF.unionAll(rbDistrictDF).distinct

// COMMAND ----------

val stockDF = ghhStockDF.unionAll(rbStockDF).distinct

// COMMAND ----------

districtDF.show(5)

// COMMAND ----------

stockDF.show(5)

// COMMAND ----------

val fileName = "/mnt/greathouse/unifiedAndClean/district.parquet"
print("Output location: " + fileName)

(districtDF.write                  // Our DataFrameWriter
  .option("compression", "snappy") // One of none, snappy, gzip, and lzo
  .mode("overwrite")               // Replace existing files
  .parquet(fileName)               // Write DataFrame to Parquet files
)

// COMMAND ----------

val fileName = "/mnt/greathouse/unifiedAndClean/stock.parquet"
print("Output location: " + fileName)

(districtDF.write                  // Our DataFrameWriter
  .option("compression", "snappy") // One of none, snappy, gzip, and lzo
  .mode("overwrite")               // Replace existing files
  .parquet(fileName)               // Write DataFrame to Parquet files
)
