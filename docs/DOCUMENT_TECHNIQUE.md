# Document Technique — OpenLMIS-DHIS2 Synchronization Middleware

**Version** : 1.0  
**Date** : 20 février 2026  
**Projet** : Middleware de synchronisation OpenLMIS → DHIS2  

---

## 1. Présentation Générale

Ce middleware Django assure la **synchronisation automatisée des données de stocks** entre la plateforme de gestion logistique **OpenLMIS** (source) et le système d'information sanitaire **DHIS2** (destination).

Le processus ETL (Extract-Transform-Load) agrège les **mouvements de stocks réels** (Stock Movements/Events) d'OpenLMIS pour calculer les consommations et réceptions mensuelles, puis pousse ces résultats vers DHIS2 sous forme de `DataValueSets`.

---

## 2. Architecture du Système

### 2.1. Schéma d'architecture globale

```mermaid
graph TB
    subgraph SOURCES_EXTERNES["Systèmes Externes"]
        OLMIS["🏥 OpenLMIS<br/><i>Gestion Logistique</i><br/>API REST + OAuth2"]
        DHIS2["📊 DHIS2<br/><i>Système d'Information Sanitaire</i><br/>API REST + Basic Auth"]
    end

    subgraph DOCKER["Stack Docker Compose"]
        subgraph APP["Application Django"]
            DJANGO["🐍 Django 5.x<br/><i>Application Web / API</i><br/>Port 8001"]
            DRF["Django REST Framework<br/><i>API Endpoints</i>"]
            ADMIN["Django Admin<br/><i>Interface d'administration</i>"]
        end
        
        subgraph SERVICES["Services de Synchronisation"]
            OLMIS_CLIENT["OpenLMIS Client<br/><i>OAuth2 + Extraction</i>"]
            AGGREGATOR["Stock Aggregator<br/><i>Pandas / NumPy</i>"]
            DHIS2_CLIENT["DHIS2 Client<br/><i>Basic Auth + Chargement</i>"]
        end

        subgraph CELERY_STACK["Couche Asynchrone Celery"]
            WORKER["⚙️ Celery Worker<br/><i>Exécution des tâches</i><br/>Concurrency: 4"]
            BEAT["⏰ Celery Beat<br/><i>Planificateur cron</i><br/>DatabaseScheduler"]
            FLOWER["🌸 Flower<br/><i>Monitoring Celery</i><br/>Port 5556"]
        end

        subgraph INFRA["Infrastructure"]
            PG["🐘 PostgreSQL 15<br/><i>Base de données</i><br/>Port 5434"]
            REDIS["🔴 Redis 7<br/><i>Cache + Backend résultats</i><br/>Port 6378"]
            RABBIT["🐰 RabbitMQ 3.12<br/><i>Message Broker</i><br/>Ports 5673 / 15673"]
        end
    end

    DJANGO --> DRF
    DJANGO --> ADMIN
    DRF --> WORKER
    BEAT -->|Planification| WORKER
    WORKER --> OLMIS_CLIENT
    WORKER --> AGGREGATOR
    WORKER --> DHIS2_CLIENT
    OLMIS_CLIENT -->|"A: Extraction"| OLMIS
    DHIS2_CLIENT -->|"C: Chargement"| DHIS2
    WORKER --> PG
    WORKER --> RABBIT
    WORKER --> REDIS
    DJANGO --> PG
    BEAT --> RABBIT
    FLOWER --> RABBIT
    FLOWER --> REDIS
```

### 2.2. Schéma du flux ETL

```mermaid
flowchart LR
    subgraph A["Étape A : Extraction"]
        A1["Authentification OAuth2<br/>(Client Credentials + Password)"]
        A2["Récupération StockCardSummaries<br/>(Pagination automatique)"]
        A3["Extraction des LineItems<br/>(Filtrage par date)"]
    end
    
    subgraph B["Étape B : Transformation"]
        B1["Conversion en DataFrame<br/>(Pandas)"]
        B2["Agrégation par produit<br/>et type de mouvement"]
        B3["Calcul des indicateurs:<br/>RECEIVED, ISSUED, ADJUSTMENT<br/>OPENING/CLOSING BALANCE"]
        B4["Mapping vers<br/>DataElements DHIS2"]
    end
    
    subgraph C["Étape C : Chargement"]
        C1["Construction du payload<br/>DataValueSet"]
        C2["POST /api/dataValueSets"]
        C3["Traitement de la réponse<br/>(ImportSummary)"]
    end

    A1 --> A2 --> A3 --> B1 --> B2 --> B3 --> B4 --> C1 --> C2 --> C3
```

### 2.3. Diagramme des modèles de données

```mermaid
erDiagram
    DHIS2Server ||--o{ FacilityMapping : "has"
    DHIS2Server ||--o{ DataSet : "has"
    FacilityMapping ||--o{ SyncLog : "logs"
    FacilityMapping ||--o{ AggregatedData : "stores"
    FacilityMapping }o--o{ SyncSchedule : "schedules"
    DataSet }o--o{ SyncSchedule : "schedules"
    DataSet ||--o{ DataElementMapping : "contains"

    DHIS2Server {
        string name UK
        url url
        string username
        string password
        boolean is_active
    }

    FacilityMapping {
        string openlmis_facility_code UK
        string openlmis_facility_name
        string dhis2_org_unit_id
        string dhis2_org_unit_name
        boolean is_active
    }

    DataElementMapping {
        string openlmis_product_code
        string indicator
        string dhis2_data_element_uid
        string dhis2_category_option_combo_uid
        boolean is_active
    }

    DataSet {
        string name UK
        string dhis2_dataset_uid UK
        string period_type
        boolean is_active
    }

    SyncSchedule {
        string name UK
        string cron_expression
        boolean is_active
        datetime last_run
        datetime next_run
    }

    SyncLog {
        string period
        string status
        int records_extracted
        int records_transformed
        int records_loaded
        int records_failed
        json details
        string celery_task_id
    }

    AggregatedData {
        string period
        string openlmis_product_code
        string indicator
        decimal value
        boolean is_synced
        datetime synced_at
    }
```

---

## 3. Stack Technologique

| Composant | Technologie | Version | Rôle |
|-----------|------------|---------|------|
| **Langage** | Python | 3.11 | Langage principal |
| **Framework Web** | Django | ≥ 5.0 | Application web & ORM |
| **API REST** | Django REST Framework | ≥ 3.14 | Endpoints API |
| **Task Queue** | Celery | ≥ 5.3 | Tâches asynchrones |
| **Scheduler** | Celery Beat | ≥ 2.5 | Planification périodique |
| **Message Broker** | RabbitMQ | 3.12 | File de messages AMQP |
| **Cache / Backend** | Redis | 7 | Cache Django + résultats Celery |
| **Base de données** | PostgreSQL | 15 | Stockage persistent |
| **Traitement données** | Pandas / NumPy | ≥ 2.1 / ≥ 1.26 | Agrégation des stocks |
| **HTTP Client** | Requests | ≥ 2.31 | Appels API |
| **Logging** | Loguru | ≥ 0.7.2 | Journalisation structurée |
| **Monitoring** | Flower | — | Supervision Celery |
| **Conteneurisation** | Docker / Docker Compose | 3.8 | Déploiement |

---

## 4. Structure du Projet

```
openlmis_dhis2/
├── config/                         # Configuration Django/Celery
│   ├── __init__.py                 # Import de l'app Celery
│   ├── settings.py                 # Settings Django (DB, APIs, Logs)
│   ├── celery.py                   # Configuration Celery Beat
│   ├── urls.py                     # Routes principales
│   ├── wsgi.py                     # Point d'entrée WSGI
│   └── asgi.py                     # Point d'entrée ASGI
│
├── sync/                           # Application principale
│   ├── models.py                   # 7 modèles Django
│   ├── tasks.py                    # 5 tâches Celery
│   ├── views.py                    # 3 vues API
│   ├── admin.py                    # Interface d'administration
│   ├── urls.py                     # Routes de l'app sync
│   ├── services/
│   │   ├── __init__.py
│   │   ├── openlmis_client.py      # Client API OpenLMIS (OAuth2)
│   │   ├── aggregator.py           # Moteur d'agrégation Pandas
│   │   └── dhis2_client.py         # Client API DHIS2
│   └── management/commands/
│       └── import_csv.py           # Commande d'import CSV
│
├── data/                           # Fichiers CSV de configuration
│   ├── facilities.csv              # Mapping Facility → OrgUnit
│   └── data_mapping.csv            # Mapping Produit → DataElement
│
├── logs/                           # Journaux (rotation quotidienne)
├── staticfiles/                    # Fichiers statiques collectés
├── docker-compose.yml              # Orchestration des 7 services
├── Dockerfile                      # Image Python 3.11-slim
├── requirements.txt                # Dépendances Python
├── manage.py                       # CLI Django
├── .env                            # Variables d'environnement
└── .env.example                    # Template des variables
```

---

## 5. Configuration et Paramétrage

### 5.1. Variables d'environnement (`.env`)

#### Django

| Variable | Description | Valeur par défaut |
|----------|-------------|-------------------|
| `SECRET_KEY` | Clé secrète Django (à changer en production) | `django-insecure-...` |
| `DEBUG` | Mode debug | `True` |
| `ALLOWED_HOSTS` | Hôtes autorisés (séparés par `,`) | `localhost,127.0.0.1` |

#### Base de données PostgreSQL

| Variable | Description | Valeur par défaut |
|----------|-------------|-------------------|
| `POSTGRES_DB` | Nom de la base | `openlmis_dhis2` |
| `POSTGRES_USER` | Utilisateur | `postgres` |
| `POSTGRES_PASSWORD` | Mot de passe | `postgres` |
| `POSTGRES_HOST` | Hôte (nom du service Docker) | `postgres` |
| `POSTGRES_PORT` | Port interne | `5432` |

#### Redis & RabbitMQ

| Variable | Description | Valeur par défaut |
|----------|-------------|-------------------|
| `REDIS_URL` | URL de connexion Redis | `redis://redis:6379/0` |
| `RABBITMQ_DEFAULT_USER` | Utilisateur RabbitMQ | `rabbitmq` |
| `RABBITMQ_DEFAULT_PASS` | Mot de passe RabbitMQ | `rabbitmq` |
| `CELERY_BROKER_URL` | URL du broker AMQP | `amqp://rabbitmq:rabbitmq@rabbitmq:5672//` |

#### API OpenLMIS

| Variable | Description | Obligatoire |
|----------|-------------|-------------|
| `OPENLMIS_BASE_URL` | URL de base de l'API OpenLMIS | ✅ |
| `OPENLMIS_CLIENT_ID` | Client ID pour l'authentification OAuth2 | ✅ |
| `OPENLMIS_CLIENT_SECRET` | Client Secret OAuth2 | ✅ |
| `OPENLMIS_USERNAME` | Nom d'utilisateur OpenLMIS | ✅ |
| `OPENLMIS_PASSWORD` | Mot de passe de l'utilisateur | ✅ |
| `OPENLMIS_PROGRAM_ID` | UUID du programme par défaut (ex: EPI) | Recommandé |

#### API DHIS2

| Variable | Description | Obligatoire |
|----------|-------------|-------------|
| `DHIS2_BASE_URL` | URL de base de l'instance DHIS2 | ✅ |
| `DHIS2_USERNAME` | Utilisateur DHIS2 | ✅ |
| `DHIS2_PASSWORD` | Mot de passe DHIS2 | ✅ |

#### Planification de la synchronisation

| Variable | Description | Valeur par défaut |
|----------|-------------|-------------------|
| `SYNC_DAY_OF_MONTH` | Jour du mois pour la synchronisation automatique | `5` |
| `SYNC_HOUR` | Heure de déclenchement (UTC) | `2` |
| `SYNC_MINUTE` | Minute de déclenchement | `0` |

### 5.2. Paramètres internes (`config/settings.py`)

| Paramètre | Valeur | Description |
|-----------|--------|-------------|
| `SYNC_CONFIG.PAGE_SIZE` | `100` | Taille de page pour la pagination API |
| `SYNC_CONFIG.MAX_RETRIES` | `3` | Nombre maximal de tentatives |
| `SYNC_CONFIG.RETRY_DELAY` | `60` s | Délai entre les tentatives |
| `CELERY_TASK_TIME_LIMIT` | `1800` s (30 min) | Temps max par tâche |
| Log rotation | Quotidienne à minuit | Rotation des fichiers de log |
| Log rétention | 30j (app), 60j (sync), 90j (errors) | Durée de conservation |

### 5.3. Fichiers CSV de mapping

#### `data/facilities.csv` — Mapping des établissements

```csv
openlmisCode,dhis2OrgUnitId
N036,DiszpKrYNg8
```

| Colonne | Description |
|---------|-------------|
| `openlmisCode` | Code de la formation sanitaire dans OpenLMIS |
| `dhis2OrgUnitId` | UID de l'unité d'organisation DHIS2 correspondante |

#### `data/data_mapping.csv` — Mapping des éléments de données

```csv
programCode,dataSetId,productCode,openlmisAttribute,dhis2DeId,dhis2CocId,desc
EPI,TuL8IOPzpHh,IVX-BCG-20-1234,beginningBalance,t99PL3gUxIl,HllvX50cXC0,Stock Initial BCG
```

| Colonne | Description |
|---------|-------------|
| `programCode` | Code du programme OpenLMIS (ex: `EPI`) |
| `dataSetId` | UID du DataSet DHIS2 cible |
| `productCode` | Code produit OpenLMIS (ex: `IVX-BCG-20-1234`) |
| `openlmisAttribute` | Attribut de stock OpenLMIS |
| `dhis2DeId` | UID du Data Element DHIS2 |
| `dhis2CocId` | UID du Category Option Combo DHIS2 |
| `desc` | Description lisible |

**Attributs OpenLMIS supportés** :

| Attribut | Indicateur | Description |
|----------|-----------|-------------|
| `beginningBalance` | `OPENING_BALANCE` | Stock initial |
| `quantityReceived` | `RECEIVED` | Quantité reçue |
| `quantityDispensed` | `ISSUED` | Quantité distribuée |
| `stockOnHand` | `STOCK_ON_HAND` | Stock disponible |
| `totalLossesAndAdjustments` | `TOTAL_ADJUSTMENT` | Ajustement net |
| `positiveAdjustment` | `ADJUSTMENT_POSITIVE` | Ajustement positif |
| `negativeAdjustment` | `ADJUSTMENT_NEGATIVE` | Ajustement négatif |
| `closingBalance` | `CLOSING_BALANCE` | Stock final |
| `expired` | `EXPIRED` | Périmé |
| `damaged` | `DAMAGED` | Endommagé |
| `lost` | `LOST` | Perdu |

---

## 6. Ports réseau

| Service | Port hôte | Port conteneur | Usage |
|---------|-----------|----------------|-------|
| **Django** | `8001` | `8000` | Application web / API |
| **PostgreSQL** | `5434` | `5432` | Base de données |
| **Redis** | `6378` | `6379` | Cache et résultats |
| **RabbitMQ AMQP** | `5673` | `5672` | Broker de messages |
| **RabbitMQ UI** | `15673` | `15672` | Console de gestion |
| **Flower** | `5556` | `5555` | Monitoring Celery |

---

## 7. Commandes

### 7.1. Déploiement initial

```bash
# 1. Cloner le projet
git clone <repo_url> openlmis_dhis2
cd openlmis_dhis2

# 2. Configurer les variables d'environnement
cp .env.example .env
nano .env         # Renseigner les credentials OpenLMIS, DHIS2, etc.

# 3. Construire et démarrer tous les services
docker-compose up -d --build

# 4. Vérifier que tous les services sont démarrés
docker-compose ps

# 5. Appliquer les migrations Django
docker-compose exec django python manage.py migrate

# 6. Créer un super-utilisateur pour l'administration
docker-compose exec django python manage.py createsuperuser

# 7. Importer les fichiers CSV de configuration
docker-compose exec django python manage.py import_csv --all
```

### 7.2. Gestion des services Docker

```bash
# Démarrer tous les services
docker-compose up -d

# Arrêter tous les services
docker-compose down

# Arrêter et supprimer les volumes (⚠️ perte de données)
docker-compose down -v

# Redémarrer un service spécifique
docker-compose restart django
docker-compose restart celery_worker
docker-compose restart celery_beat

# Reconstruire un service après modification du code
docker-compose up -d --build django

# Voir l'état des services
docker-compose ps
```

### 7.3. Logs et monitoring

```bash
# Voir les logs de tous les services
docker-compose logs -f

# Logs d'un service spécifique
docker-compose logs -f django
docker-compose logs -f celery_worker
docker-compose logs -f celery_beat

# Logs avec un nombre limité de lignes
docker-compose logs --tail=100 django

# Accéder aux fichiers de logs internes
docker-compose exec django ls -la /app/logs/
docker-compose exec django cat /app/logs/app_2026-02-20.log
docker-compose exec django cat /app/logs/errors_2026-02-20.log
docker-compose exec django cat /app/logs/sync_2026-02-20.log
```

### 7.4. Base de données

```bash
# Appliquer les migrations
docker-compose exec django python manage.py migrate

# Créer de nouvelles migrations après modification des modèles
docker-compose exec django python manage.py makemigrations

# Vérifier l'état des migrations
docker-compose exec django python manage.py showmigrations

# Accéder au shell PostgreSQL
docker-compose exec postgres psql -U postgres -d openlmis_dhis2

# Accéder au shell Django ORM
docker-compose exec django python manage.py shell

# Vérifier la configuration
docker-compose exec django python manage.py check
```

### 7.5. Import des données CSV

```bash
# Importer tous les fichiers CSV
docker-compose exec django python manage.py import_csv --all

# Importer uniquement les facilities
docker-compose exec django python manage.py import_csv --facilities

# Importer uniquement les mappings de données
docker-compose exec django python manage.py import_csv --mappings

# Spécifier un répertoire personnalisé
docker-compose exec django python manage.py import_csv --all --data-dir /chemin/vers/csv
```

### 7.6. Synchronisation manuelle

```bash
# Déclencher une sync pour toutes les facilities (mois précédent)
curl -X POST http://localhost:8001/api/trigger/ \
  -H "Content-Type: application/json"

# Déclencher une sync pour une période spécifique
curl -X POST http://localhost:8001/api/trigger/ \
  -H "Content-Type: application/json" \
  -d '{"period": "202601"}'

# Sync pour des facilities spécifiques
curl -X POST http://localhost:8001/api/trigger/ \
  -H "Content-Type: application/json" \
  -d '{"period": "202601", "facilities": ["N036"]}'

# Consulter le statut des dernières syncs
curl http://localhost:8001/api/status/

# Consulter les logs de sync
curl http://localhost:8001/api/logs/
curl "http://localhost:8001/api/logs/?facility=N036&period=202601&status=FAILED"

# Déclencher via le shell Django
docker-compose exec django python manage.py shell -c \
  "from sync.tasks import sync_all_facilities_task; sync_all_facilities_task.delay('202601')"

# Relancer les syncs échouées
docker-compose exec django python manage.py shell -c \
  "from sync.tasks import retry_failed_syncs; retry_failed_syncs.delay('202601')"
```

### 7.7. Celery & Monitoring

```bash
# Vérifier la connectivité Celery
docker-compose exec django celery -A config inspect ping

# Voir les tâches actives
docker-compose exec django celery -A config inspect active

# Voir les tâches planifiées
docker-compose exec django celery -A config inspect scheduled

# Purger la file de tâches
docker-compose exec django celery -A config purge

# Tâche de debug
docker-compose exec django celery -A config call config.celery.debug_task
```

---

## 8. Interfaces de monitoring

| Interface | URL | Credentials |
|-----------|-----|------------|
| **Django Admin** | `http://localhost:8001/admin/` | Superuser créé avec `createsuperuser` |
| **Flower** (Celery) | `http://localhost:5556/` | Pas d'authentification par défaut |
| **RabbitMQ Management** | `http://localhost:15673/` | `rabbitmq` / `rabbitmq` |

### Administration Django

L'interface d'administration permet de gérer :

- **DHIS2 Servers** : Serveurs DHIS2 configurés
- **Facility Mappings** : Correspondances OpenLMIS ↔ DHIS2
- **Data Element Mappings** : Correspondances Produit+Indicateur ↔ DataElement
- **DataSets** : Configuration des DataSets DHIS2
- **Sync Schedules** : Planifications de synchronisation
- **Sync Logs** : Historique des synchronisations (lecture seule)
- **Aggregated Data** : Données agrégées en attente ou synchronisées

---

## 9. Journalisation (Logging)

Le système utilise **Loguru** avec 3 fichiers de logs :

| Fichier | Niveau | Rétention | Contenu |
|---------|--------|-----------|---------|
| `logs/app_YYYY-MM-DD.log` | `DEBUG` | 30 jours | Logs généraux de l'application |
| `logs/sync_YYYY-MM-DD.log` | `INFO` | 60 jours | Opérations de synchronisation uniquement |
| `logs/errors_YYYY-MM-DD.log` | `ERROR` | 90 jours | Erreurs critiques |

**Rotation** : quotidienne à minuit, compression `.gz`.

Exemples de logs :
```
2026-02-20 02:00:01 | INFO     | sync.tasks:sync_facility_task:88 | Start Sync for Facility N036 (Month: Jan 2026)
2026-02-20 02:00:03 | DEBUG    | sync.tasks:sync_facility_task:177 | Aggregated IVX-BCG-20-1234 - RECEIVED: 150
2026-02-20 02:00:05 | ERROR    | sync.tasks:sync_facility_task:260 | [N036] Sync failed with error: API error: 401
```

---

## 10. Maintenance

### 10.1. Tâches régulières

| Tâche | Fréquence | Commande / Action |
|-------|-----------|-------------------|
| Vérifier l'état des services | Quotidien | `docker-compose ps` |
| Consulter les logs d'erreur | Quotidien | `docker-compose logs --tail=50 celery_worker` |
| Vérifier les syncs échouées | Après chaque sync | Consulter Django Admin → Sync Logs |
| Relancer les syncs échouées | Après chaque échec | `retry_failed_syncs.delay('YYYYMM')` |
| Mettre à jour les CSV | Selon besoins | Modifier `data/*.csv` puis `import_csv --all` |
| Nettoyer les anciens logs | Automatique | Tâche `cleanup_old_logs` (90 jours) |
| Sauvegarder la BDD | Hebdomadaire | `docker-compose exec postgres pg_dump ...` |

### 10.2. Sauvegarde et restauration

```bash
# Sauvegarder la base de données
docker-compose exec postgres pg_dump -U postgres openlmis_dhis2 > backup_$(date +%Y%m%d).sql

# Restaurer une sauvegarde
docker-compose exec -T postgres psql -U postgres openlmis_dhis2 < backup_20260220.sql

# Sauvegarder les volumes Docker
docker run --rm -v openlmis_dhis2_postgres_data:/data -v $(pwd):/backup \
  alpine tar czf /backup/postgres_data_backup.tar.gz /data
```

### 10.3. Mise à jour de l'application

```bash
# 1. Tirer les dernières modifications
git pull origin main

# 2. Reconstruire les images Docker
docker-compose up -d --build

# 3. Appliquer les nouvelles migrations
docker-compose exec django python manage.py migrate

# 4. Collecter les fichiers statiques
docker-compose exec django python manage.py collectstatic --noinput

# 5. Réimporter les CSV si modifiés
docker-compose exec django python manage.py import_csv --all
```

### 10.4. Dépannage

| Problème | Diagnostic | Solution |
|----------|------------|----------|
| Service ne démarre pas | `docker-compose logs <service>` | Vérifier `.env` et dépendances |
| Erreur OAuth2 (401) | Vérifier les logs du worker | Valider `OPENLMIS_*` dans `.env` |
| Erreur DHIS2 (403/409) | Vérifier `sync_YYYY-MM-DD.log` | Valider les UIDs dans `data_mapping.csv` |
| Tâches Celery bloquées | `celery inspect active` | `celery purge` puis restart worker |
| BDD pleine | `docker-compose exec postgres psql ...` | Lancer `cleanup_old_logs` |
| Redis indisponible | `docker-compose logs redis` | `docker-compose restart redis` |
| RabbitMQ saturé | Console RabbitMQ `:15673` | Purger les queues |

---

## 11. Sécurité

> [!CAUTION]
> En **production**, les paramètres suivants **doivent** être modifiés :

| Paramètre | Action requise |
|-----------|---------------|
| `SECRET_KEY` | Générer une clé unique et sécurisée |
| `DEBUG` | Mettre à `False` |
| `ALLOWED_HOSTS` | Restreindre au domaine de production |
| `POSTGRES_PASSWORD` | Utiliser un mot de passe fort |
| `RABBITMQ_DEFAULT_PASS` | Utiliser un mot de passe fort |
| Ports exposés | Limiter l'exposition réseau (firewall) |
| Flower | Ajouter une authentification |
| HTTPS | Configurer un reverse proxy (Nginx + TLS) |
