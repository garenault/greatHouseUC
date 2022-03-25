// Databricks notebook source
// MAGIC %run "./Get DFs"

// COMMAND ----------

val silverStockPath = "/mnt/greathouse/silver/stock.parquet"
val silverDistrictPath = "/mnt/greathouse/silver/district.parquet"

// COMMAND ----------

// DBTITLE 1,•	Analyse the average price per nomber of rooms
import org.apache.spark.sql.functions.col

val stockDF = spark.read.parquet(silverStockPath)

val avgPriceByRoomsDF = stockDF
                        .groupBy( col("Rooms") )
                        .avg("price")



// COMMAND ----------

display(avgPriceByRoomsDF)

// COMMAND ----------

avgPriceByRoomsDF.write.format("parquet").saveAsTable("avgPriceByRooms")

// COMMAND ----------

// DBTITLE 1,•	Analyse the different indicators the lowest, highest, average of price from each district 
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructType, IntegerType}




val stockDF = spark.read.parquet(silverStockPath).withColumnRenamed("longitude", "longitudeStock").withColumnRenamed("latitude", "latitudeStock")
val districtDF = spark.read.parquet(silverDistrictPath).withColumnRenamed("longitude", "longitudeDistrict").withColumnRenamed("latitude", "latitudeDistrict")

        

val allDF = stockDF.join(districtDF, stockDF("longitudeStock") === districtDF("longitudeDistrict") &&
    stockDF("latitudeStock") === districtDF("latitudeDistrict"))

val groupedDF = allDF.groupBy(col("latitudeDistrict"), col("longitudeDistrict")).agg(min("price"), max("price"), avg("price"))



// COMMAND ----------

display(groupedDF)

// COMMAND ----------

groupedDF.write.format("parquet").saveAsTable("minMaxAvgPriceByDistrict")

// COMMAND ----------

// DBTITLE 1,•	Which kind of house is most in the market (such as how many rooms, how many bedrooms)?
val houseMostInMarketDF = stockDF.groupBy(col("Rooms"), col("Bedrooms")).count().orderBy(col("count").desc)

// COMMAND ----------

display(houseMostInMarketDF)

// COMMAND ----------

houseMostInMarketDF.write.format("parquet").saveAsTable("houseMostInMarket")

// COMMAND ----------

// DBTITLE 1,•	For different price range, analyse the difference of houses (nb of rooms, bedrooms)?
val houseDetailByPriceDF = stockDF.select("price","Rooms","Bedrooms").orderBy(col("price").asc)

// COMMAND ----------

display(houseDetailByPriceDF.where(col("price") > 0 && col("price") < 1000))

// COMMAND ----------

houseDetailByPriceDF.write.format("parquet").saveAsTable("houseDetailByPrice")

// COMMAND ----------

// DBTITLE 1,•	(optionnal) Could you build a price map by the address of house (geolocation information)?


// COMMAND ----------



// COMMAND ----------

// DBTITLE 1,•	What other interesting analysis could you think of?

