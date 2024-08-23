import os
import requests
import json 
import logging
import xml.etree.ElementTree as ET
from django.shortcuts import render
from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from .models import UploadedFile

logger = logging.getLogger(__name__)

def homepage(request):
    return HttpResponse("Welcome to the homepage!")

@csrf_exempt
def get_files(request):
    files = UploadedFile.objects.all().values('name', 'graph_id', 'size', 'id')
    file_list = list(files)
    return JsonResponse({'files': file_list})

@csrf_exempt
def create_database(request):
    if request.method == 'POST':
        data = request.POST
        namespace = data.get('namespace')
        properties = {
            'com.bigdata.rdf.store.DataLoader': 'com.bigdata.rdf.data.RDFDataLoader',
            'com.bigdata.rdf.store.DataLoader.context': 'com.bigdata.rdf.data.RDFDataLoaderContext',
            'com.bigdata.rdf.sail.isolates': 'true',
            'com.bigdata.rdf.sail.quads': 'true',
            'com.bigdata.rdf.sail.axioms': 'true',
            'com.bigdata.rdf.sail.includeInferred': 'true',
            'com.bigdata.rdf.sail.incremental': 'false',
        }

        url = f"http://172.17.0.1:9999/blazegraph/namespace/{namespace}"
        response = requests.post(url, json={'properties': properties})

        if response.status_code == 200:
            return JsonResponse({'message': 'Database created successfully'})
        else:
            return JsonResponse({'error': 'Failed to create database', 'details': response.text}, status=response.status_code)

    return JsonResponse({'error': 'Invalid request method'}, status=405)

@csrf_exempt
def create_namespace(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            print("Received data:", data)

            namespace = data.get('namespace')
            if not namespace:
                return JsonResponse({"error": "Namespace is required."}, status=400)
            
            properties = data.get('properties', {})
            
            properties_str = "\n".join(f"{key}={value}" for key, value in properties.items())
            properties_str += f"\ncom.bigdata.rdf.sail.namespace={namespace}"

            headers = {"Content-Type": "text/plain"}
            
            url = "http://172.17.0.1:9999/blazegraph/namespace"
            
            print(f"Sending request to: {url}")
            print(f"Request payload: {properties_str}")
            
            response = requests.post(url, headers=headers, data=properties_str)
            
            print(f"Response status code: {response.status_code}")
            print(f"Response content: {response.text}")
            
            if response.status_code in [200, 201]:
                return JsonResponse({"message": f"Namespace '{namespace}' created successfully."})
            else:
                return JsonResponse({"error": f"Failed to create namespace. Status code: {response.status_code}, Response: {response.text}"}, status=response.status_code)
        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON payload."}, status=400)
        except requests.RequestException as e:
            print(f"Request exception: {str(e)}")
            return JsonResponse({"error": f"An error occurred: {str(e)}"}, status=500)
    
    return JsonResponse({"error": "Invalid request method."}, status=405)

@csrf_exempt
def upload_ttl(request):
    if request.method == 'POST':
        graph_id = request.POST.get('graph_id')
        ttl_file = request.FILES.get('file')

        if not ttl_file:
            return JsonResponse({"error": "No file provided."}, status=400)

        size = ttl_file.size

        try:
            UploadedFile.objects.create(
                name=ttl_file.name,
                graph_id=graph_id,
                size=size,
            )

            url = f"http://127.0.0.1:8000/api/"
            headers = {"Content-Type": "text/turtle"}

            response = requests.post(url, headers=headers, data=ttl_file.read())

            if response.status_code == 200:
                return JsonResponse({"message": f"File '{ttl_file.name}' uploaded successfully."})
            else:
                return JsonResponse({"error": f"Failed to upload file to the server. Status code: {response.status_code}, Response: {response.text}"}, status=response.status_code)
        
        except Exception as e:
            return JsonResponse({"error": f"An error occurred: {str(e)}"}, status=500)
    
    return JsonResponse({"error": "Invalid request method."}, status=405)

@csrf_exempt
def connect_database(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            ip_address = data.get('ipAddress')
            port = data.get('port')
            database_type = data.get('databaseType')

            if not ip_address or not port or not database_type:
                return JsonResponse({"error": "Missing required fields"}, status=400)

            url = f"http://{ip_address}:{port}/blazegraph/namespace/{database_type}/sparql"
            response = requests.get(url)

            if response.status_code == 200:
                return JsonResponse({"success": True, "message": "Connected successfully"})
            else:
                return JsonResponse({"success": False, "message": "Failed to connect"}, status=response.status_code)
        except Exception as e:
            return JsonResponse({"success": False, "message": str(e)}, status=500)
    return JsonResponse({"error": "Invalid request method."}, status=405)

@csrf_exempt
def get_active_database(request):
    if request.method == 'GET':
        try:
            logger.info("Attempting to fetch active database")
            url = "http://172.17.0.1:9999/blazegraph/namespace"
            logger.info(f"Sending GET request to: {url}")
            response = requests.get(url, timeout=10)
            logger.info(f"Received response with status code: {response.status_code}")
            response.raise_for_status()
            
            content_type = response.headers.get('Content-Type', '')
            if 'application/rdf+xml' in content_type:
                root = ET.fromstring(response.content)
                
                namespaces = []
                for desc in root.findall('.//{http://www.w3.org/1999/02/22-rdf-syntax-ns#}Description'):
                    namespace = desc.find('.//{http://www.bigdata.com/rdf#/features/KB/}Namespace')
                    if namespace is not None:
                        namespaces.append(namespace.text)
                
                if namespaces:
                    active_database = namespaces[0]  # Assuming the first namespace is active
                    logger.info(f"Found active database: {active_database}")
                    return JsonResponse({'active_database': active_database})
                else:
                    logger.warning("No namespaces found")
                    return JsonResponse({"message": "No namespaces found"}, status=404)
            else:
                logger.warning(f"Unexpected content type: {content_type}")
                logger.info(f"Response content: {response.text}")
                return JsonResponse({"message": "Blazegraph returned an unexpected response format"}, status=500)

        except requests.RequestException as e:
            logger.error(f"Error connecting to Blazegraph: {str(e)}")
            return JsonResponse({"message": f"Failed to connect to Blazegraph: {str(e)}"}, status=500)
        except ET.ParseError as e:
            logger.error(f"Error parsing XML from Blazegraph: {str(e)}")
            return JsonResponse({"message": "Failed to parse Blazegraph response"}, status=500)
        except Exception as e:
            logger.error(f"Unexpected error in get_active_database: {str(e)}", exc_info=True)
            return JsonResponse({"message": f"An unexpected error occurred: {str(e)}"}, status=500)
    return JsonResponse({"error": "Invalid request method."}, status=405)

@csrf_exempt
def get_active_repository(request):
    if request.method == 'GET':
        try:
            url = "http://172.17.0.1:9999/blazegraph/namespace"
            response = requests.get(url)
            
            content_type = response.headers.get('Content-Type', '')
            if 'application/rdf+xml' in content_type:
                root = ET.fromstring(response.content)
                
                namespaces = []
                for desc in root.findall('.//{http://www.w3.org/1999/02/22-rdf-syntax-ns#}Description'):
                    namespace = desc.find('.//{http://www.bigdata.com/rdf#/features/KB/}Namespace')
                    if namespace is not None:
                        namespaces.append(namespace.text)
                
                if namespaces:
                    # For simplicity, we're considering all namespaces as active repositories
                    # You may need to adjust this logic based on your specific requirements
                    return JsonResponse({'active_repositories': namespaces})
                else:
                    return JsonResponse({"message": "No active repositories found"}, status=404)
            else:
                logger.warning(f"Unexpected content type: {content_type}")
                return JsonResponse({"message": "Blazegraph returned an unexpected response format"}, status=500)
        except ET.ParseError as e:
            logger.error(f"Error parsing XML from Blazegraph: {str(e)}")
            return JsonResponse({"message": "Failed to parse Blazegraph response"}, status=500)
        except Exception as e:
            return JsonResponse({"message": f"Failed to fetch active repositories: {str(e)}"}, status=500)
    return JsonResponse({"error": "Invalid request method."}, status=405)