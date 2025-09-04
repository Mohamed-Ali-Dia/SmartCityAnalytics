package com.smartcity.iot

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.broadcast
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object DataProcessor {
  // === UDF extractSensorMetrics ===
  // Cette UDF prend en entrée :
  // - le timestamp (format yyyyMMddHHmmss)
  // - la température et le vent
  // Elle renvoie un tuple contenant :
  //   1) time_slot (matin, après-midi, soir, nuit)
  //   2) is_peak_hour (1 si heure de pointe, sinon 0)
  //   3) weather_alert (1 si conditions extrêmes, sinon 0)
  //   4) day_of_week (jour de la semaine en texte)
  private val extractSensorMetrics = udf { (ts: String, temp: java.lang.Double, wind: java.lang.Double) =>
    try {
      // Découpage de la date/heure
      val year = ts.substring(0,4).toInt
      val month = ts.substring(4,6).toInt
      val day = ts.substring(6,8).toInt
      val hour = ts.substring(8,10).toInt

      // Crénaux horaires
      val slot = hour match {
        case h if 0 to 5 contains h => "Night"
        case h if 6 to 11 contains h => "Morning"
        case h if 12 to 17 contains h => "Afternoon"
        case _ => "Evening"
      }

      // Heure de pointe
      val isPeak = if ((7 to 9 contains hour) || (17 to 19 contains hour)) 1 else 0

      // Vérification météo
      val t = Option(temp).map(_.toDouble).getOrElse(Double.NaN)
      val w = Option(wind).map(_.toDouble).getOrElse(Double.NaN)
      val weatherAlert = if ((!t.isNaN && (t < 0 || t > 35)) || (!w.isNaN && w > 50)) 1 else 0

      // Jour de la semaine
      val dateStr = f"$year%04d-$month%02d-$day%02d"
      val date = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
      val dow = date.getDayOfWeek.toString.capitalize

      (slot, isPeak, weatherAlert, dow)
    } catch {
      case _: Throwable => ("Unknown", 0, 0, "Unknown")
    }
  }

  // === Validations ===
  // Vérification des contraintes métier pour chaque type de données IoT
  def validateSensors(df: DataFrame): DataFrame = {
    df.filter(
      col("latitude").between(-90, 90) &&
      col("longitude").between(-180, 180) &&
      col("status").isin("ACTIVE","MAINTENANCE","OFFLINE")
    )
  }

  def validateWeather(df: DataFrame): DataFrame = {
    df.filter(
      col("temperature").between(-50, 60) &&
      col("humidity").between(0, 100) &&
      col("pressure") > 800
    )
  }

  def validateTraffic(df: DataFrame): DataFrame = {
    df.filter(
      col("vehicle_count") >= 0 &&
      col("avg_speed").between(0, 200)
    )
  }

  def validateZones(df: DataFrame): DataFrame = {
    df.filter(
      col("population") >= 0 && col("area_km2") > 0
    )
  }

  // === Enrichment ===
  // Jointure des datasets (capteurs, météo, trafic, zones)
  // Application de l’UDF extractSensorMetrics
  // Ajout de métriques avec Window function
  def enrichSensorData(sensors: DataFrame, weather: DataFrame, traffic: DataFrame, zones: DataFrame)
                      (implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

     // Jointure capteurs ↔ zones (broadcast car zones = petite table)
    val sensorsZones = sensors.join(broadcast(zones), Seq("zone_id"), "left")

    // Jointure capteurs ↔ météo
    val sensorsWeather = sensorsZones
      .join(weather, Seq("sensor_id"), "left")
      .withColumn("date", coalesce(weather("date"), lit(null:String)))

    // Jointure capteurs + météo ↔ trafic
    val sensorsWeatherTraffic = sensorsWeather
      .join(traffic, Seq("sensor_id", "date"), "left")

    // Application de l’UDF extractSensorMetrics
    val withUdf = sensorsWeatherTraffic
      .withColumn("_metrics", extractSensorMetrics(
        col("timestamp"),
        col("temperature"),
        col("wind_speed")
      ))
      .withColumn("time_slot", $"_metrics._1")
      .withColumn("is_peak_hour", $"_metrics._2")
      .withColumn("weather_alert", $"_metrics._3")
      .withColumn("day_of_week", $"_metrics._4")
      .drop("_metrics")

    // Ajout du nombre de capteurs actifs par zone (fenêtre analytique)
    val wByZone = Window.partitionBy("zone_id")
    withUdf.withColumn("active_sensor_count_by_zone",
      count(when(col("status") === "ACTIVE", 1)).over(wByZone)
    )
  }
}