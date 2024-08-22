from django import forms

class TTLFileUploadForm(forms.Form):
    ttl_file = forms.FileField(label="Upload TTL File")