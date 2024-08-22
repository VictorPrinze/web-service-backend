from rest_framework import serializers

class TTLFileUploadSerializer(serializers.Serializer):
    ttl_file = serializers.FileField()