package com.smartcity.iot

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// Classe responsable de la lecture (ingestion) des données IoT
// Elle centralise les méthodes de lecture multi-sources (JSON, CSV, Parquet)
// et applique les schémas nécessaires pour typer correctement les colonnes.
class DataReader(spark: SparkSession, conf: Config) {

  // Récupération de la section "input" du fichier config.conf
  private val input = conf.getConfig("smartcity.data.input")

  // Schéma explicite pour les données météo
  private val weatherSchema = StructType(Seq(
    StructField("reading_id", StringType, nullable = false),
    StructField("sensor_id", StringType, nullable = false),
    StructField("timestamp", StringType, nullable = false), // yyyyMMddHHmmss
    StructField("temperature", DoubleType, nullable = true),
    StructField("humidity", DoubleType, nullable = true),
    StructField("pressure", DoubleType, nullable = true),
    StructField("wind_speed", DoubleType, nullable = true),
    StructField("rainfall", DoubleType, nullable = true)
  ))

  // Schéma explicite pour les capteurs
  private val sensorsSchema = StructType(Seq(
    StructField("sensor_id", StringType, nullable = false),
    StructField("sensor_type", StringType, nullable = false),
    StructField("zone_id", StringType, nullable = false),
    StructField("installation_date", StringType, nullable = true), // yyyyMMdd
    StructField("latitude", DoubleType, nullable = true),
    StructField("longitude", DoubleType, nullable = true),
    StructField("status", StringType, nullable = true)
  ))

  // Schéma explicite pour les événements de trafic
  private val trafficSchema = StructType(Seq(
    StructField("event_id", StringType, nullable = false),
    StructField("sensor_id", StringType, nullable = false),
    StructField("timestamp", StringType, nullable = false),
    StructField("vehicle_count", IntegerType, nullable = true),
    StructField("avg_speed", DoubleType, nullable = true),
    StructField("traffic_density", StringType, nullable = true),
    StructField("incident_type", StringType, nullable = true)
  ))

  // Schéma explicite pour les zones urbaines
  private val zonesSchema = StructType(Seq(
    StructField("zone_id", StringType, nullable = false),
    StructField("zone_name", StringType, nullable = true),
    StructField("zone_type", StringType, nullable = true),
    StructField("population", LongType, nullable = true),
    StructField("area_km2", DoubleType, nullable = true),
    StructField("district", StringType, nullable = true)
  ))

  // === Méthodes de lecture ===

  // Lecture des capteurs (JSON)
  def readSensors(): DataFrame = {
    val path = input.getString("sensors")
    spark.read
      .schema(sensorsSchema)
      .option("multiline", "true")
      .json(path)
      .withColumn("installation_dt", to_date(col("installation_date"), "yyyyMMdd"))
  }

  // Lecture des données météo (CSV)
  def readWeather(): DataFrame = {
    val path = input.getString("weather")
    spark.read
      .schema(weatherSchema)
      .option("header", "true")
      .csv(path)
      .withColumn("event_ts", to_timestamp(col("timestamp"), "yyyyMMddHHmmss"))
      .withColumn("date", date_format(col("event_ts"), "yyyy-MM-dd"))
  }

  // Lecture des données trafic (Parquet prioritaire, sinon CSV)
  def readTraffic(): DataFrame = {
    val path = input.getString("traffic")
    val base = path.replaceAll("\\.parquet$", "")
    val csvCandidate = if (path.endsWith(".parquet")) base + ".csv" else path + ".csv"
    try {
      // Lecture Parquet (optimisé)
      spark.read
        .schema(trafficSchema)
        .parquet(path)
        .withColumn("event_ts", to_timestamp(col("timestamp"), "yyyyMMddHHmmss"))
        .withColumn("date", date_format(col("event_ts"), "yyyy-MM-dd"))
    } catch {
      case _: Throwable =>
        // Fallback en CSV si le fichier parquet n’existe pas
        spark.read
          .schema(trafficSchema)
          .option("header", "true")
          .csv(csvCandidate)
          .withColumn("event_ts", to_timestamp(col("timestamp"), "yyyyMMddHHmmss"))
          .withColumn("date", date_format(col("event_ts"), "yyyy-MM-dd"))
    }
  }

  // Lecture des zones urbaines (CSV simple)
  def readZones(): DataFrame = {
    val path = input.getString("zones")
    spark.read
      .schema(zonesSchema)
      .option("header", "true")
      .csv(path)
  }
}