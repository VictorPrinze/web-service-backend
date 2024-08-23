from django.db import models

class UploadedFile(models.Model):
    name = models.CharField(max_length=255)
    graph_id = models.CharField(max_length=50)
    size = models.CharField(max_length=50)
    uploaded_at = models.DateTimeField(auto_now_add=True)