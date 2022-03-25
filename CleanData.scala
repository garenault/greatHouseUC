// Databricks notebook source
// MAGIC %run "./MountBlobContainer"

// COMMAND ----------

// DBTITLE 1,Create Dataframe from files in bronze storage
val optionsMap = Map("inferSchema"->"true", "header"->"true")

val ghhDistrictDF = spark.read.options(optionsMap).csv(ghhDistrictPath)
val ghhStockDF = spark.read.options(optionsMap).csv(ghhStockPath)
val rbDistrictDF = spark.read.options(optionsMap).csv(rbDistrictPath)
val rbStockDF = spark.read.options(optionsMap).csv(rbStockPath)
val reAdDF = spark.read.options(optionsMap).csv(reAdPath)

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

print("Output location: " + silverDistrictPath)

(districtDF.write                  // Our DataFrameWriter
  .option("compression", "snappy") // One of none, snappy, gzip, and lzo
  .mode("overwrite")               // Replace existing files
  .parquet(silverDistrictPath)     // Write DataFrame to Parquet files
)

// COMMAND ----------

print("Output location: " + silverStockPath)

(stockDFPrice.write                // Our DataFrameWriter
  .option("compression", "snappy") // One of none, snappy, gzip, and lzo
  .mode("overwrite")               // Replace existing files
  .parquet(silverStockPath)        // Write DataFrame to Parquet files
)
