"""
Celery configuration for OpenLMIS-DHIS2 Middleware.
"""
import os
from celery import Celery
from celery.schedules import crontab
from decouple import config

# Set the default Django settings module
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

# Create the Celery app
app = Celery('openlmis_dhis2')

# Load config from Django settings
app.config_from_object('django.conf:settings', namespace='CELERY')

# Auto-discover tasks in all installed apps
app.autodiscover_tasks()

# =============================================================================
# CELERY BEAT SCHEDULE
# =============================================================================
# Default schedule: Run on the 5th of each month at 02:00 AM
# This can be overridden via the Django admin using django-celery-beat

SYNC_DAY = config('SYNC_DAY_OF_MONTH', default=5, cast=int)
SYNC_HOUR = config('SYNC_HOUR', default=2, cast=int)
SYNC_MINUTE = config('SYNC_MINUTE', default=0, cast=int)

app.conf.beat_schedule = {
    'monthly-sync-all-facilities': {
        'task': 'sync.tasks.sync_all_facilities_task',
        'schedule': crontab(
            day_of_month=SYNC_DAY,
            hour=SYNC_HOUR,
            minute=SYNC_MINUTE,
        ),
        'args': (),  # Period will be calculated automatically (previous month)
    },
}

app.conf.timezone = 'UTC'


@app.task(bind=True, ignore_result=True)
def debug_task(self):
    """Debug task to test Celery connectivity."""
    print(f'Request: {self.request!r}')
