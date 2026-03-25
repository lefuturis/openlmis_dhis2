"""
URL configuration for OpenLMIS-DHIS2 Middleware.
"""
from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/', include('sync.urls')),
]
