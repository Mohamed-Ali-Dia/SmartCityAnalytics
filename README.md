# SmartCityAnalytics (Spark + Scala)

Projet de rattrapage — **Data Engineering: Spark & Scala**  
Thème: **Système d'analyse de données IoT pour Smart City**

## Objectif
Mettre en place un pipeline complet de traitement de données IoT (capteurs météo, trafic, zones urbaines) en utilisant **Apache Spark** et **Scala**, afin de :
- Valider et nettoyer les données brutes
- Enrichir avec des métriques dérivées
- Détecter des alertes en temps réel
- Produire des agrégations utiles pour la ville intelligente
- Sauvegarder dans plusieurs formats (JSON & Parquet)

## Arborescence

SmartCityAnalytics/
├── build.sbt
├── README.md
└── src/main/scala/
   └── com/smartcity/
      ├── models/
      │  └── Models.scala
      └── iot/
         ├── DataReader.scala
         ├── DataProcessor.scala
         ├── IoTAnalytics.scala
         └── MainApplication.scala
└── src/main/resources/
   ├── config.conf
   └── datasets/
      ├── sensors.json
      ├── weather_data.csv
      ├── traffic_events.parquet
      └── city_zones.csv

## Choix techniques et justifications

### 1. **Lecture multi-source avec schéma explicite**
- **Pourquoi ?**  
  Les données proviennent de sources hétérogènes (JSON, CSV, Parquet).  
  Utiliser un **schéma explicite** permet :
  - de garantir la cohérence des types
  - d’éviter l’inférence coûteuse et imprécise
  - de documenter la structure des données
- **Implémentation :** dans `DataReader.scala`, chaque dataset a un `StructType` défini.

### 2. **Validation des données IoT**
- **Pourquoi ?**  
  Les données IoT peuvent être bruitées (capteurs défaillants, valeurs incohérentes).  
  La validation permet d’écarter les enregistrements aberrants avant traitement.
- **Règles appliquées :**
  - Latitude / longitude dans des bornes réalistes
  - Température entre -50 et 60°C
  - Vitesse moyenne de trafic ≤ 200 km/h
  - Zones avec population ≥ 0 et superficie > 0
- **Implémentation :** dans `DataProcessor.scala` (fonctions `validateX`).

### 3. **Enrichissement avec UDF + fenêtres analytiques**
- **Pourquoi ?**
  - L’UDF `extractSensorMetrics` permet de dériver des indicateurs utiles à partir des timestamps (créneau horaire, jour de la semaine, heure de pointe).
  - Les **Window functions** permettent d’ajouter des métriques globales par zone (nombre de capteurs actifs).
- **Bénéfice :** plus de valeur ajoutée pour l’analyse, sans modifier la donnée brute.

### 4. **Optimisations Spark**
- **Broadcast join** (zones) :  
  Les zones urbaines sont une petite table → Spark diffuse cette table aux workers, ce qui accélère les jointures.
- **Caching** (zones, capteurs) :  
  Tables de référence fréquemment utilisées → éviter de relire depuis disque.
- **Persist MEMORY_AND_DISK_SER** :  
  Les résultats intermédiaires (`enriched`, `alerts`, `dailyAgg`) sont réutilisés pour plusieurs sorties → on les met en cache compressé mémoire+disque.
- **Unpersist** :  
  Nettoyage mémoire après utilisation, pour éviter l’accumulation.

### 5. **Partitionnement et formats de sortie**
- **Partitionnement par `date` et `zone_id`** :  
  Optimise la lecture ultérieure (ex. filtrage par période ou zone).
- **Multi-format (JSON & Parquet)** :  
  - JSON → facile à explorer, compatible avec de nombreux outils
  - Parquet → format optimisé pour l’analytique (colonnaire, compression)

### 6. **Détection d’alertes**
- **Règles simples mais pratiques :**
  - Capteurs en panne ou en maintenance
  - Conditions météo extrêmes
  - Trafic critique ou incidents (accident, embouteillage, travaux)
- **Pourquoi ?**  
  Ces alertes permettent de simuler un **usage opérationnel temps réel** de la plateforme.

### 7. **Agrégations journalières**
- Moyennes météo (température, humidité)
- Vitesse moyenne du trafic
- Nombre total de véhicules
- Nombre de capteurs actifs dans la zone
- **But :** fournir des indicateurs utiles pour la **planification urbaine** et la **gestion quotidienne de la ville**.

## Lancement

Depuis la racine du projet :
$bash > sbt clean compile run