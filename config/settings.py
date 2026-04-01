"""
Django settings for OpenLMIS-DHIS2 Middleware project.
"""
import os
import sys
from pathlib import Path
from decouple import config
from loguru import logger

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = config('SECRET_KEY', default='django-insecure-change-me-in-production')

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = config('DEBUG', default=False, cast=bool)

ALLOWED_HOSTS = config('ALLOWED_HOSTS', default='localhost,127.0.0.1').split(',')

import os

# Application definition
INSTALLED_APPS = [
    'jazzmin',
    'daphne',
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    # Third-party apps
    'rest_framework',
    'django_celery_beat',
    'django_celery_results',
    'channels',
    # Local apps
    'sync',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'whitenoise.middleware.WhiteNoiseMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

# =============================================================================
# ENVIRONMENT & PRODUCTION SECURITY
# =============================================================================
ENVIRONMENT = config('ENVIRONMENT', default='development')

if ENVIRONMENT == 'production':
    SECURE_SSL_REDIRECT = config('SECURE_SSL_REDIRECT', default=True, cast=bool)
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True
    SECURE_BROWSER_XSS_FILTER = True
    SECURE_CONTENT_TYPE_NOSNIFF = True
    SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTO', 'https')

# Trusted origins for CSRF (e.g. https://lmis-dhis2.example.com)
trusted_origins = config('CSRF_TRUSTED_ORIGINS', default='')
if trusted_origins:
    CSRF_TRUSTED_ORIGINS = [origin.strip() for origin in trusted_origins.split(',') if origin.strip()]

ROOT_URLCONF = 'config.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'config.wsgi.application'
ASGI_APPLICATION = 'config.asgi.application'

# Database
# https://docs.djangoproject.com/en/5.0/ref/settings/#databases
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': config('POSTGRES_DB', default='openlmis_dhis2'),
        'USER': config('POSTGRES_USER', default='postgres'),
        'PASSWORD': config('POSTGRES_PASSWORD', default='postgres'),
        'HOST': config('POSTGRES_HOST', default='localhost'),
        'PORT': config('POSTGRES_PORT', default='5432'),
    },
    'openlmis_reporting': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': config('OPENLMIS_REPORTING_DB', default='open_lmis_reporting'),
        'USER': config('OPENLMIS_REPORTING_USER', default=config('POSTGRES_USER', default='postgres')),
        'PASSWORD': config('OPENLMIS_REPORTING_PASSWORD', default=config('POSTGRES_PASSWORD', default='postgres')),
        'HOST': config('OPENLMIS_REPORTING_HOST', default=config('POSTGRES_HOST', default='localhost')),
        'PORT': config('OPENLMIS_REPORTING_PORT', default=config('POSTGRES_PORT', default='5432')),
    }
}

# Password validation
AUTH_PASSWORD_VALIDATORS = [
    {'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator'},
    {'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator'},
    {'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator'},
    {'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator'},
]

# Internationalization
LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_TZ = True

# Static files (CSS, JavaScript, Images)
STATIC_URL = 'static/'
STATIC_ROOT = BASE_DIR / 'staticfiles'

# Default primary key field type
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# =============================================================================
# CELERY CONFIGURATION
# =============================================================================
CELERY_BROKER_URL = config('CELERY_BROKER_URL', default='amqp://rabbitmq:rabbitmq@localhost:5672//')
CELERY_RESULT_BACKEND = config('REDIS_URL', default='redis://localhost:6379/0')
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TIMEZONE = TIME_ZONE
CELERY_TASK_TRACK_STARTED = True
CELERY_TASK_TIME_LIMIT = 30 * 60  # 30 minutes
CELERY_RESULT_EXTENDED = True

# Celery Beat - Use database scheduler
CELERY_BEAT_SCHEDULER = 'django_celery_beat.schedulers:DatabaseScheduler'

# =============================================================================
# REDIS CACHE CONFIGURATION
# =============================================================================
CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.redis.RedisCache',
        'LOCATION': config('REDIS_URL', default='redis://localhost:6379/0'),
    }
}

# =============================================================================
# DJANGO CHANNELS CONFIGURATION
# =============================================================================
CHANNEL_LAYERS = {
    'default': {
        'BACKEND': 'channels_redis.core.RedisChannelLayer',
        'CONFIG': {
            'hosts': [config('REDIS_URL', default='redis://localhost:6379/0')],
        },
    },
}

# =============================================================================
# OPENLMIS API CONFIGURATION
# =============================================================================
OPENLMIS_CONFIG = {
    'BASE_URL': config('OPENLMIS_BASE_URL', default='https://openlmis.example.org'),
    'CLIENT_ID': config('OPENLMIS_CLIENT_ID', default=''),
    'CLIENT_SECRET': config('OPENLMIS_CLIENT_SECRET', default=''),
    'USERNAME': config('OPENLMIS_USERNAME', default=''),
    'PASSWORD': config('OPENLMIS_PASSWORD', default=''),
    'DEFAULT_PROGRAM_ID': config('OPENLMIS_PROGRAM_ID', default=''),
    'TOKEN_ENDPOINT': '/api/oauth/token',
    'STOCK_CARDS_ENDPOINT': '/api/stockCards',
    'STOCK_CARD_SUMMARIES_ENDPOINT': '/api/stockCardSummaries',
    'STOCK_EVENTS_ENDPOINT': '/api/stockEvents',
}

# =============================================================================
# SYNC CONFIGURATION
# =============================================================================
SYNC_CONFIG = {
    'DAY_OF_MONTH': config('SYNC_DAY_OF_MONTH', default=5, cast=int),
    'HOUR': config('SYNC_HOUR', default=2, cast=int),
    'MINUTE': config('SYNC_MINUTE', default=0, cast=int),
    'PAGE_SIZE': 100,  # Pagination size for API requests
    'MAX_RETRIES': 3,
    'RETRY_DELAY': 60,  # Seconds between retries
}

# =============================================================================
# DHIS2 API CONFIGURATION
# =============================================================================
DHIS2_CONFIG = {
    'BASE_URL': config('DHIS2_BASE_URL', default='https://dhis2.example.org'),
    'USERNAME': config('DHIS2_USERNAME', default='admin'),
    'PASSWORD': config('DHIS2_PASSWORD', default='district'),
    'API_ENDPOINT': '/api',
    'DATA_VALUES_ENDPOINT': '/api/dataValueSets',
    'ORG_UNITS_ENDPOINT': '/api/organisationUnits',
}

# =============================================================================
# LOGURU CONFIGURATION
# =============================================================================
# Remove default logger
logger.remove()

# Log format
LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
    "<level>{message}</level>"
)

# Console logging
logger.add(
    sys.stderr,
    format=LOG_FORMAT,
    level="DEBUG" if DEBUG else "INFO",
    colorize=True,
)

# File logging with rotation
LOG_DIR = BASE_DIR / 'logs'
LOG_DIR.mkdir(exist_ok=True)

logger.add(
    LOG_DIR / "app_{time:YYYY-MM-DD}.log",
    format=LOG_FORMAT,
    level="DEBUG",
    rotation="00:00",  # Rotate at midnight
    retention="30 days",
    compression="gz",
)

# Error-specific log file
logger.add(
    LOG_DIR / "errors_{time:YYYY-MM-DD}.log",
    format=LOG_FORMAT,
    level="ERROR",
    rotation="00:00",
    retention="90 days",
    compression="gz",
)

# Sync-specific log file
logger.add(
    LOG_DIR / "sync_{time:YYYY-MM-DD}.log",
    format=LOG_FORMAT,
    level="INFO",
    rotation="00:00",
    retention="60 days",
    compression="gz",
    filter=lambda record: "sync" in record["name"].lower(),
)

# =============================================================================
# REST FRAMEWORK CONFIGURATION
# =============================================================================
REST_FRAMEWORK = {
    'DEFAULT_PERMISSION_CLASSES': [
        'rest_framework.permissions.IsAuthenticated',
    ],
    'DEFAULT_PAGINATION_CLASS': 'rest_framework.pagination.PageNumberPagination',
    'PAGE_SIZE': 50,
}

# =============================================================================
# DATA FILES DIRECTORY
# =============================================================================
DATA_DIR = BASE_DIR / 'data'

# =============================================================================
# JAZZMIN CONFIGURATION
# =============================================================================
JAZZMIN_SETTINGS = {
    # title of the window (Will default to current_admin_site.site_title if absent or None)
    "site_title": "OpenLMIS → DHIS2",

    # Title on the login screen (19 chars max)
    "site_header": "OpenLMIS-DHIS2 Sync",

    # Title on the brand (19 chars max)
    "site_brand": "Sync Portal",

    # Welcome text on the login screen
    "welcome_sign": "Connexion au portail OpenLMIS-DHIS2",

    # Copyright on the footer
    "copyright": "MinSanté",

    # Field name on user model that contains avatar ImageField/URLField/Charfield or a callable that receives the user
    "user_avatar": None,

    # Top Menu
    "topmenu_links": [
        {"name": "Home",  "url": "admin:index", "permissions": ["auth.view_user"]},
        {"name": "🚀 Lancer la Synchronisation DHIS2", "url": "/admin/sync/manual-sync/", "new_window": False},
    ],

    # Custom icons for model admin classes (string form of "app_label.model_name" or "app_label")
    "icons": {
        "auth": "fas fa-users-cog",
        "auth.user": "fas fa-user",
        "auth.Group": "fas fa-users",
        "sync.DHIS2Server": "fas fa-server",
        "sync.FacilityMapping": "fas fa-hospital",
        "sync.DataElementMapping": "fas fa-exchange-alt",
        "sync.DataSet": "fas fa-database",
        "sync.SyncSchedule": "fas fa-clock",
        "sync.SyncLog": "fas fa-list",
        "sync.AggregatedData": "fas fa-chart-bar",
        "django_celery_beat.PeriodicTask": "fas fa-tasks",
        "django_celery_results.TaskResult": "fas fa-clipboard-check",
    },
    
    # Icons that are used when one is not manually specified
    "default_icon_parents": "fas fa-chevron-circle-right",
    "default_icon_children": "fas fa-circle",
    
    # Hide these apps when generating side menu e.g (auth)
    "hide_apps": [],

    # Hide these models when generating side menu (e.g auth.user)
    "hide_models": [],
    
    # Order of apps in the menu
    "order_with_respect_to": ["sync", "auth"],
}

JAZZMIN_UI_TWEAKS = {
    "theme": "pulse",  # Modern clean theme
    "dark_mode_theme": "darkly",
    "navbar": "navbar-dark",
    "navbar_fixed": True,
    "sidebar": "sidebar-dark-primary",
    "sidebar_fixed": True,
    "sidebar_nav_child_indent": True,
    "sidebar_nav_compact_style": False,
    "sidebar_nav_legacy_style": False,
    "sidebar_nav_flat_style": False,
    "brand_small_text": False,
    "brand_colour": "navbar-primary",
    "accent": "accent-primary",
    "footer_fixed": False,
    "body_small_text": False,
}
