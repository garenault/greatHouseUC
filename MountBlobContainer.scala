// Databricks notebook source
// MAGIC %md
// MAGIC # Mount Azure Blob Storage with all the files

// COMMAND ----------

// DBTITLE 1,Mount blob storage

if(dbutils.fs.mounts().count(x=> x.mountPoint == "/mnt/greathouse") == 0){
  dbutils.fs.mount(
    source = "wasbs://greathouseblob@greathousegresa.blob.core.windows.net",
    mountPoint = "/mnt/greathouse",
    extraConfigs = Map("fs.azure.account.key.greathousegresa.blob.core.windows.net" -> dbutils.secrets.get(scope = "AKV-greatHouse", key = "SAkey1")))  
} else {
  print("Already mounted")
  
}
  


// COMMAND ----------

// DBTITLE 1,List all paths of files in Bronze storage
val ghhDistrictPath = "dbfs:/mnt/greathouse/bronze/GreatHouse Holdings District.csv"
val ghhStockPath = "dbfs:/mnt/greathouse/bronze/Stock GreatHouse Holdings.csv"
val rbDistrictPath = "dbfs:/mnt/greathouse/bronze/Roger&Brothers District.csv"
val rbStockPath = "dbfs:/mnt/greathouse/bronze/Stock Roger&Brothers.csv"
val reAdPath = "dbfs:/mnt/greathouse/bronze/Real Estate Ad.csv"

// COMMAND ----------

// DBTITLE 1,List paths of silver Storage
val silverDistrictPath = "/mnt/greathouse/silver/district.parquet"
val silverStockPath = "/mnt/greathouse/silver/stock.parquet"

// COMMAND ----------

// DBTITLE 1,To Unmount the blob storage
//dbutils.fs.unmount("/mnt/greathouse")
