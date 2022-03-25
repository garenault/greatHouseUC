// Databricks notebook source
// MAGIC %md
// MAGIC # Mount Azure Blob Storage with all the files

// COMMAND ----------


if(dbutils.fs.mounts().count(x=> x.mountPoint == "/mnt/greathouse") == 0){
  dbutils.fs.mount(
    source = "wasbs://greathouseblob@greathousegresa.blob.core.windows.net",
    mountPoint = "/mnt/greathouse",
    extraConfigs = Map("fs.azure.account.key.greathousegresa.blob.core.windows.net" -> dbutils.secrets.get(scope = "AKV-greatHouse", key = "SAkey1")))  
} else {
  print("Already mounted")
  
}
  


// COMMAND ----------

// DBTITLE 1,List all files
display(dbutils.fs.ls("/mnt/greathouse/"))

// COMMAND ----------

// MAGIC %md
// MAGIC # Create all dataframes for each files

// COMMAND ----------

val ghhDistrictPath = "dbfs:/mnt/greathouse/bronze/GreatHouse Holdings District.csv"
val ghhStockPath = "dbfs:/mnt/greathouse/bronze/Stock GreatHouse Holdings.csv"
val rbDistrictPath = "dbfs:/mnt/greathouse/bronze/Roger&Brothers District.csv"
val rbStockPath = "dbfs:/mnt/greathouse/bronze/Stock Roger&Brothers.csv"
val reAdPath = "dbfs:/mnt/greathouse/bronze/Real Estate Ad.csv"

// COMMAND ----------

val optionsMap = Map("inferSchema"->"true", "header"->"true")

val ghhDistrictDF = spark.read.options(optionsMap).csv(ghhDistrictPath)
val ghhStockDF = spark.read.options(optionsMap).csv(ghhStockPath)
val rbDistrictDF = spark.read.options(optionsMap).csv(rbDistrictPath)
val rbStockDF = spark.read.options(optionsMap).csv(rbStockPath)
val reAdDF = spark.read.options(optionsMap).csv(reAdPath)

// COMMAND ----------

display(ghhDistrictDF)
//display(ghhStockDF)
//display(rbDistrictDF)
//display(rbStockDF)
//display(reAdDF)

// COMMAND ----------

// DBTITLE 1,To Unmount the blob storage
//dbutils.fs.unmount("/mnt/greathouse")
