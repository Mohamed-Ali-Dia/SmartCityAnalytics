package com.smartcity.models

// === Case class Sensor ===
// Définition du schéma logique d’un capteur IoT
case class Sensor(
  sensor_id: String,
  sensor_type: String,
  zone_id: String,
  installation_date: String,
  latitude: Double,
  longitude: Double,
  status: String // ACTIVE, MAINTENANCE, OFFLINE
)

// === Case class WeatherReading ===
// Données météorologiques liées à un capteur
case class WeatherReading(
  reading_id: String,
  sensor_id: String,
  timestamp: String, // yyyyMMddHHmmss
  temperature: Double,
  humidity: Double,
  pressure: Double,
  wind_speed: Double,
  rainfall: Double
)

// === Case class TrafficEvent ===
// Événement de trafic détecté par un capteur
case class TrafficEvent(
  event_id: String,
  sensor_id: String,
  timestamp: String,
  vehicle_count: Int,
  avg_speed: Double,
  traffic_density: String, // LOW, MEDIUM, HIGH, CRITICAL
  incident_type: String // NONE, ACCIDENT, ROADWORK, JAM
)

// === Case class CityZone ===
// Métadonnées décrivant une zone urbaine
case class CityZone(
  zone_id: String,
  zone_name: String,
  zone_type: String, // RESIDENTIAL, COMMERCIAL, INDUSTRIAL, PARK
  population: Long,
  area_km2: Double,
  district: String
)