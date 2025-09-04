package com.smartcity.iot

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

// Cet objet regroupe les fonctions analytiques appliquées aux données IoT
// - Génération d'alertes
// - Agrégations journalières par zone
object IoTAnalytics {

  /** === Génération d’alertes ===
  * Règles définies :
  * - Capteur offline ou en maintenance
  * - Conditions météo extrêmes (colonne weather_alert issue de la UDF)
  * - Trafic critique ou incident (accident, travaux, embouteillage)
  */
  def generateAlerts(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    df.withColumn("alert_type",
      when(col("status") === "OFFLINE", lit("SENSOR_OFFLINE"))
        .when(col("status") === "MAINTENANCE", lit("SENSOR_MAINTENANCE"))
        .when(col("weather_alert") === 1, lit("WEATHER_EXTREME"))
        .when(col("traffic_density").isin("CRITICAL"), lit("TRAFFIC_CRITICAL"))
        .when(col("incident_type").isin("ACCIDENT","ROADWORK","JAM"), col("incident_type"))
        .otherwise(lit(null:String))
    ).filter(col("alert_type").isNotNull)   // On ne conserve que les lignes avec alerte
  }

  /** === Agrégations journalières par zone ===
  * Indicateurs calculés :
  * - Température et humidité moyennes
  * - Vitesse moyenne du trafic
  * - Nombre total de véhicules détectés
  * - Nombre de capteurs actifs par zone
  */
  def dailyZoneAggregates(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    df.groupBy("zone_id","zone_name","date")
      .agg(
        avg("temperature").as("avg_temp"),
        avg("humidity").as("avg_humidity"),
        avg("avg_speed").as("avg_speed"),
        sum(coalesce(col("vehicle_count"), lit(0))).as("vehicles_total"),
        max("active_sensor_count_by_zone").as("active_sensors_in_zone")
      )
  }
}