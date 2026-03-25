# OpenLMIS-DHIS2 Synchronization Middleware

Middleware Django pour synchroniser les mouvements de stocks entre OpenLMIS (source) et DHIS2 (destination).

## 🎯 Objectif

Cette application agrège les **mouvements de stocks réels** (Stock Movements/Events) d'OpenLMIS pour calculer les consommations et réceptions mensuelles, puis pousse ces résultats vers DHIS2.

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Docker Compose Stack                          │
├─────────────┬─────────────┬─────────────┬─────────────┬─────────┤
│  PostgreSQL │    Redis    │  RabbitMQ   │   Django    │ Celery  │
│   (Base)    │   (Cache)   │  (Broker)   │   (API)     │ (Tasks) │
└─────────────┴─────────────┴─────────────┴─────────────┴─────────┘
                                │
                    ┌───────────┴───────────┐
                    │                       │
              ┌─────▼─────┐           ┌─────▼─────┐
              │ OpenLMIS  │           │   DHIS2   │
              │   API     │           │    API    │
              └───────────┘           └───────────┘
```

## 🚀 Démarrage Rapide

### 1. Configuration

```bash
# Copier le fichier d'environnement
cp .env.example .env

# Éditer avec vos credentials
nano .env
```

### 2. Lancer les services

```bash
# Construire et démarrer tous les services
docker-compose up -d

# Vérifier les logs
docker-compose logs -f
```

### 3. Initialiser la base de données

```bash
# Exécuter les migrations
docker-compose exec django python manage.py migrate

# Créer un superuser
docker-compose exec django python manage.py createsuperuser

# Importer les fichiers CSV de configuration
docker-compose exec django python manage.py import_csv --all
```

## 📁 Structure du Projet

```
openlmis_dhis2/
├── config/                 # Configuration Django/Celery
│   ├── settings.py        # Settings avec Loguru
│   ├── celery.py          # Configuration Celery Beat
│   └── urls.py
├── sync/                   # Application principale
│   ├── models.py          # Modèles Django (Servers, Mappings, Logs)
│   ├── tasks.py           # Tâches Celery
│   ├── views.py           # API endpoints
│   ├── services/
│   │   ├── openlmis_client.py   # Client API OpenLMIS
│   │   ├── aggregator.py        # Moteur d'agrégation Pandas
│   │   └── dhis2_client.py      # Client API DHIS2
│   └── management/commands/
│       └── import_csv.py        # Import des CSV
├── data/                   # Fichiers CSV de configuration
│   ├── dhis2.servers.csv
│   ├── dhis2.shared_facilities.csv
│   ├── dhis2.data_elements.csv
│   ├── dhis2.datasets.csv
│   └── dhis2.schedules.csv
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

## 📊 Fichiers CSV de Configuration

| Fichier | Description |
|---------|-------------|
| `dhis2.servers.csv` | Credentials des serveurs DHIS2 |
| `dhis2.shared_facilities.csv` | Mapping Facility OpenLMIS → Org Unit DHIS2 |
| `dhis2.data_elements.csv` | Mapping Produit + Indicateur → DataElement |
| `dhis2.datasets.csv` | Configuration des DataSets DHIS2 |
| `dhis2.schedules.csv` | Planification des synchronisations |

## 🔄 Workflow de Synchronisation

### Étape A: Extraction (OpenLMIS)
- Authentification OAuth2
- Récupération des stock cards avec pagination
- Filtrage par période (mois précédent)

### Étape B: Transformation
- Agrégation avec Pandas par produit et type de mouvement
- Calcul des indicateurs: `RECEIVED`, `ISSUED`, `ADJUSTMENT`, `OPENING_BALANCE`, `CLOSING_BALANCE`
- Mapping vers les DataElements DHIS2

### Étape C: Chargement (DHIS2)
- Construction du payload DataValueSet
- POST sur `/api/dataValueSets`
- Gestion des erreurs et conflits

## 🖥️ Endpoints API

| Endpoint | Méthode | Description |
|----------|---------|-------------|
| `/api/status/` | GET | Statut des dernières syncs |
| `/api/trigger/` | POST | Déclencher une sync manuelle |
| `/api/logs/` | GET | Historique des syncs |

### Déclencher une sync manuellement

```bash
curl -X POST http://localhost:8000/api/trigger/ \
  -H "Content-Type: application/json" \
  -d '{"period": "202511"}'
```

## 📝 Logs

Les logs sont gérés par Loguru avec rotation automatique:

```
logs/
├── app_2026-01-20.log      # Logs généraux
├── errors_2026-01-20.log   # Erreurs uniquement
└── sync_2026-01-20.log     # Logs de synchronisation
```

Exemples de logs:
```
INFO: Start Sync for Facility D001 (Month: Nov 2025)
DEBUG: Aggregated BCG - Received: 150, Issued: 40
ERROR: Mapping missing for Product 'Paracetamol' in data_elements.csv
```

## 🔧 Commandes Utiles

```bash
# Voir les logs Celery
docker-compose logs -f celery_worker

# Accéder au shell Django
docker-compose exec django python manage.py shell

# Relancer une sync échouée
docker-compose exec django python manage.py shell -c \
  "from sync.tasks import retry_failed_syncs; retry_failed_syncs.delay('202511')"

# Monitoring Celery via Flower
# Accéder à http://localhost:5555
```

## 🔐 Variables d'Environnement

| Variable | Description | Défaut |
|----------|-------------|--------|
| `OPENLMIS_BASE_URL` | URL de l'API OpenLMIS | - |
| `OPENLMIS_CLIENT_ID` | Client ID OAuth2 | - |
| `OPENLMIS_CLIENT_SECRET` | Client Secret OAuth2 | - |
| `OPENLMIS_USERNAME` | Utilisateur OpenLMIS | - |
| `OPENLMIS_PASSWORD` | Mot de passe OpenLMIS | - |
| `SYNC_DAY_OF_MONTH` | Jour de sync mensuelle | 5 |
| `SYNC_HOUR` | Heure de sync | 2 |

## 📈 Monitoring

- **RabbitMQ**: http://localhost:15672 (rabbitmq/rabbitmq)
- **Flower**: http://localhost:5555
- **Django Admin**: http://localhost:8000/admin/

## 🛠️ Développement

```bash
# Installer les dépendances localement
pip install -r requirements.txt

# Lancer les tests
python manage.py test

# Vérifier le code
python manage.py check
```

## 📄 License

MIT License
