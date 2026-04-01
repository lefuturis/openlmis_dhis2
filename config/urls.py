"""
URL configuration for OpenLMIS-DHIS2 Middleware.
"""
from django.urls import path, include
from sync.admin import admin_site

urlpatterns = [
    path('admin/', admin_site.urls),
    path('api/', include('sync.urls')),
]
