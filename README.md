# Projet Streaming Temps Réel avec Kafka + Spark

Ce projet conteneurisé illustre un pipeline complet d'ingestion et de traitement de données en temps réel, conforme au chapitre 7 : *Data Ingestion et Streaming Temps Réel*.

## Objectifs pédagogiques

- Comprendre la différence entre batch processing et stream processing
- Maîtriser les concepts Kafka (Producer, Consumer, Topic)
- Utiliser Spark Structured Streaming pour le traitement continu
- Construire un pipeline Big Data moderne : `Sources → Kafka → Spark → Console`

## Architecture du projet
[Producer Python] → [Kafka Topic: web_logs] → [Spark Streaming]→ [Analyses temps réel]
├── Pages populaires
├── Erreurs 404
└── Utilisateurs actifs

## 📁 Structure des fichiers
projet-streaming/
├── docker-compose.yml # Orchestration des services
├── producer/
│ ├── Dockerfile # Image Python pour le producer
│ └── producer.py # Génération continue de logs JSON
├── spark/
│ ├── Dockerfile # Image Spark avec dépendances
│ └── streaming_app.py # Lecture Kafka + transformations
└── README.md


## 🛠️ Prérequis

- Docker (version 20.10+)
- Docker Compose (version 2.0+)
- 4 Go de RAM minimum disponibles
- Ports 9092 (Kafka) libres

## 🚀 Lancer le projet

### 1. Cloner ou créer la structure du projet

```bash
mkdir projet-streaming
cd projet-streaming
# Créer les fichiers docker-compose.yml, producer/, spark/ avec les contenus fournis

### 2. Démarrer tous les services
docker-compose up --build
### 3. Observer les logs en temps réel
- Les logs du producer s'affichent : production d'un message JSON par seconde
- Les logs de Spark montrent les résultats des analyses toutes les 10 secondes

### 4. Arrêter le pipeline
- Ctrl + C
- docker-compose down
