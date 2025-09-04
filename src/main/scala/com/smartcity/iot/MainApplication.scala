package com.smartcity.iot

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._

// Objet principal qui orchestre le pipeline complet IoT
// Étapes :
// 1. Configuration Spark
// 2. Ingestion des données
// 3. Validation
// 4. Enrichissement
// 5. Analyse (alertes, agrégations)
// 6. Sauvegarde multi-format
object MainApplication {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load("config.conf").getConfig("smartcity")
    val appName = config.getString("app_name")
    val sparkConf = config.getConfig("spark")
    val master   = if (sparkConf.hasPath("master")) sparkConf.getString("master") else "local[*]"
    val shuffle  = if (sparkConf.hasPath("shuffle_partitions")) sparkConf.getInt("shuffle_partitions") else 4

    // Initialisation de la session Spark
    implicit val spark: SparkSession = SparkSession.builder()
      .appName(appName)
      .master(master)
      .config("spark.sql.shuffle.partitions", shuffle)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Ingestion (via DataReader)
    val reader = new DataReader(spark, ConfigFactory.load(config))
    val sensorsRaw = reader.readSensors()
    val weatherRaw = reader.readWeather()
    val trafficRaw = reader.readTraffic()
    val zonesRaw   = reader.readZones()

    // Mise en cache des tables de référence
    val zones = zonesRaw.cache()
    val sensors = sensorsRaw.cache()

    // Validation
    val sensorsVal = DataProcessor.validateSensors(sensors)
    val weatherVal = DataProcessor.validateWeather(weatherRaw)
    val trafficVal = DataProcessor.validateTraffic(trafficRaw)
    val zonesVal   = DataProcessor.validateZones(zones)

    // Enrichissement
    val enriched = DataProcessor.enrichSensorData(sensorsVal, weatherVal, trafficVal, zonesVal)

    // Analyse : alertes et agrégations journalière
    val alerts = IoTAnalytics.generateAlerts(enriched)
    val dailyAgg = IoTAnalytics.dailyZoneAggregates(enriched)

    // Configuration sortie
    val outConf = config.getConfig("data").getConfig("output")
    val outPath = outConf.getString("path")
    val formats = outConf.getStringList("formats")

    // Ajout de la colonne "date" pour partitionner les sorties
    val enrichedOut = enriched.withColumn("date", coalesce(col("date"), to_date(col("event_ts"))))

    // Mise en mémoire disque (persist) pour éviter recomputations
    enrichedOut.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER)
    alerts.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER)
    dailyAgg.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER)

    // Sauvegarde dans plusieurs formats (JSON, Parquet)
    formats.asScala.foreach { fmt =>
      enrichedOut.write.mode("overwrite").partitionBy("date","zone_id").format(fmt).save(s"$outPath/enriched")
      alerts.write.mode("overwrite").partitionBy("date","zone_id").format(fmt).save(s"$outPath/alerts")
      dailyAgg.write.mode("overwrite").partitionBy("date","zone_id").format(fmt).save(s"$outPath/daily_agg")
    }

    // Libération mémoire
    enrichedOut.unpersist()
    alerts.unpersist()
    dailyAgg.unpersist()
    zones.unpersist()
    sensors.unpersist()

    println(s"Pipeline completed. Results written to: $outPath")
    spark.stop()
  }
}