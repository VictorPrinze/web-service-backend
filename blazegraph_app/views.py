from django.http import JsonResponse
import requests
from rdflib import Graph
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .serializers import TTLFileUploadSerializer
from django.conf import settings

@api_view(['POST'])
def upload_ttl(request):
    serializer = TTLFileUploadSerializer(data=request.data)

    if serializer.is_valid():
        ttl_file = serializer.validated_data['ttl_file']

        # Parse the TTL file
        g = Graph()
        g.parse(ttl_file, format="ttl")

        # Serialize the graph to a string in N-Triples format
        rdf_data = g.serialize(format="nt")

        # Send the RDF data to Blazegraph
        response = requests.post(
            settings.BLAZEGRAPH_URL,  # Use the URL from environment variables
            data=rdf_data,
            headers={"Content-Type": "application/n-triples"}
        )

        if response.status_code == 200:
            return Response({'message': 'Upload successful'}, status=status.HTTP_200_OK)
        else:
            return Response({'error': response.text}, status=status.HTTP_400_BAD_REQUEST)
    else:
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
@api_view(['GET'])
def get_ttl(request):
    query = """
    SELECT ?subject ?predicate ?object
    WHERE {
      ?subject ?predicate ?object
    }
    LIMIT 100
    """

    response = requests.get(
        settings.BLAZEGRAPH_URL,  # Use the URL from environment variables
        params={'query': query},
        headers={"Accept": "application/n-triples"}
    )

    if response.status_code == 200:
        return Response(response.text, status=status.HTTP_200_OK, content_type="application/n-triples")
    else:
        return Response({'error': response.text}, status=status.HTTP_400_BAD_REQUEST)