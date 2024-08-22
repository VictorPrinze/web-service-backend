from django.urls import path
from .views import upload_ttl
from .views import get_ttl

urlpatterns = [
    path('upload/', upload_ttl, name='upload_ttl'),
    path('get-ttl/', get_ttl, name='get_ttl')
]
