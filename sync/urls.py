"""
URL configuration for the sync app.
"""
from django.urls import path
from . import views

app_name = 'sync'

urlpatterns = [
    path('status/', views.sync_status, name='status'),
    path('trigger/', views.trigger_sync, name='trigger'),
    path('logs/', views.sync_logs, name='logs'),
]
