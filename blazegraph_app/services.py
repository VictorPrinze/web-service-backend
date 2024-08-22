from SPARQLWrapper import SPARQLWrapper, JSON
import requests

class BlazegraphService:
    def __init__(self, endpoint_url):
        self.endpoint_url = endpoint_url
        self.sparql = SPARQLWrapper(endpoint_url)

    # Method to execute SPARQL queries
    def query(self, query_string):
        self.sparql.setQuery(query_string)
        self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()
        return results

    # Method to create a new namespace
    def create_namespace(self, namespace_name, properties=None):
        namespace_name = namespace_name or "default_namespace"
        if properties is None:
            properties = {
                "com.bigdata.rdf.sail.isolatableIndices": "false",
                "com.bigdata.rdf.store.AbstractTripleStore.textIndex": "true",
                "com.bigdata.rdf.store.AbstractTripleStore.justify": "true",
                "com.bigdata.rdf.store.AbstractTripleStore.quads": "false",
                "com.bigdata.namespace." + namespace_name + ".spo.com.bigdata.btree.BTree.branchingFactor": "1024"
            }

        response = requests.post(
            f"{self.endpoint_url}/blazegraph/namespace",
            data=properties
        )
        if response.status_code == 201:
            return f"Namespace '{namespace_name}' created successfully."
        else:
            return f"Failed to create namespace. Status code: {response.status_code}"

    # Method to upload TTL files
    def upload_ttl(self, ttl_file_content, namespace_name):
        ttl_url = f"{self.endpoint_url}/blazegraph/namespace/{namespace_name}/sparql"

        response = requests.post(
            ttl_url,
            data=ttl_file_content,
            headers={"Content-Type": "application/x-turtle"}
        )

        if response.status_code == 200:
            return "TTL file uploaded successfully."
        else:
            return f"Failed to upload TTL file. Status code: {response.status_code}"

# Example usage of the updated BlazegraphService
def fetch_data_from_blazegraph():
    endpoint_url = "http://localhost:9999/blazegraph/namespace/GraphDatabaseService/sparql"
    blazegraph_service = BlazegraphService(endpoint_url)

    query_string = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT ?subject ?predicate ?object
        WHERE {
            ?subject ?predicate ?object
        }
        LIMIT 10
    """
    results = blazegraph_service.query(query_string)
    return results

def create_new_namespace(namespace_name):
    endpoint_url = "http://localhost:9999/blazegraph"
    blazegraph_service = BlazegraphService(endpoint_url)
    result = blazegraph_service.create_namespace(namespace_name)
    return result

def upload_ttl_to_namespace(ttl_file_content, namespace_name):
    endpoint_url = "http://localhost:9999/blazegraph"
    blazegraph_service = BlazegraphService(endpoint_url)
    result = blazegraph_service.upload_ttl(ttl_file_content, namespace_name)
    return result
