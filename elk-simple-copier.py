import requests
import json
from requests.auth import HTTPBasicAuth

class ElasticsearchMigration:
    def __init__(self, source_host, dest_host, source_auth, dest_auth):
        self.source_host = source_host.rstrip('/')
        self.dest_host = dest_host.rstrip('/')
        self.source_auth = HTTPBasicAuth(*source_auth)
        self.dest_auth = HTTPBasicAuth(*dest_auth)
        self.headers = {'Content-Type': 'application/json'}

    def get_source_index_info(self, index_name):
        """Get both settings and mappings from source index"""
        # Get settings
        settings_url = f"{self.source_host}/{index_name}/_settings"
        settings_response = requests.get(settings_url, auth=self.source_auth)
        settings_response.raise_for_status()
        
        # Get mappings
        mappings_url = f"{self.source_host}/{index_name}/_mapping"
        mappings_response = requests.get(mappings_url, auth=self.source_auth)
        mappings_response.raise_for_status()
        
        settings = settings_response.json()[index_name]['settings']
        mappings = mappings_response.json()[index_name]['mappings']
        
        print("Source Settings:", json.dumps(settings, indent=2))
        print("Source Mappings:", json.dumps(mappings, indent=2))
        
        return settings, mappings

    def create_index_with_settings(self, index_name, source_settings, source_mappings):
        """Create index with source settings and mappings"""
        url = f"{self.dest_host}/{index_name}"
        
        # Delete existing index if it exists
        try:
            requests.delete(url, auth=self.dest_auth)
        except:
            pass

        # Create basic settings
        settings = {
            "index": {
                "number_of_shards": source_settings.get("index", {}).get("number_of_shards", "1"),
                "number_of_replicas": source_settings.get("index", {}).get("number_of_replicas", "1"),
                "mapping": {
                    "total_fields": {
                        "limit": 2000
                    }
                }
            }
        }
        
        # Create config
        config = {
            "settings": settings,
            "mappings": source_mappings
        }
        
        print("Creating index with config:", json.dumps(config, indent=2))
        
        response = requests.put(url, auth=self.dest_auth, headers=self.headers, json=config)
        response.raise_for_status()
        print(f"Created index {index_name} with settings and mappings")
        return response.json()

    def reindex_data(self, source_index, batch_size=1000):
        """Reindex data using scroll API"""
        print(f"Starting reindexing for {source_index}")
        scroll_url = f"{self.source_host}/{source_index}/_search?scroll=5m"
        scroll_body = {
            "size": batch_size,
            "query": {"match_all": {}}
        }
        
        response = requests.post(scroll_url, auth=self.source_auth, headers=self.headers, json=scroll_body)
        response.raise_for_status()
        
        scroll_id = response.json()['_scroll_id']
        hits = response.json()['hits']['hits']
        total_docs = 0
        
        while hits:
            bulk_body = ""
            for hit in hits:
                action = {"index": {"_index": source_index, "_id": hit['_id']}}
                bulk_body += json.dumps(action) + "\n"
                bulk_body += json.dumps(hit['_source']) + "\n"
            
            bulk_url = f"{self.dest_host}/_bulk"
            response = requests.post(bulk_url, auth=self.dest_auth, headers=self.headers, data=bulk_body)
            
            if response.status_code != 200:
                print(f"Error in bulk operation: {response.text}")
            else:
                resp_json = response.json()
                if resp_json.get('errors', False):
                    print("Bulk operation had errors:", json.dumps(resp_json, indent=2))
            
            total_docs += len(hits)
            print(f"Migrated {total_docs} documents")
            
            scroll_url = f"{self.source_host}/_search/scroll"
            scroll_body = {
                "scroll": "5m",
                "scroll_id": scroll_id
            }
            response = requests.post(scroll_url, auth=self.source_auth, headers=self.headers, json=scroll_body)
            response.raise_for_status()
            
            scroll_id = response.json()['_scroll_id']
            hits = response.json()['hits']['hits']
        
        requests.delete(f"{self.source_host}/_search/scroll", 
                       auth=self.source_auth, 
                       headers=self.headers, 
                       json={"scroll_id": scroll_id})
        
        print(f"Completed reindexing {total_docs} documents")

    def migrate_index(self, index_name):
        """Migrate index with settings"""
        print(f"Starting migration for index: {index_name}")
        try:
            # Get source settings and mappings
            settings, mappings = self.get_source_index_info(index_name)
            print(f"Retrieved settings and mappings from source index {index_name}")
            
            # Create index with settings
            self.create_index_with_settings(index_name, settings, mappings)
            print(f"Created destination index with settings")
            
            # Reindex data
            self.reindex_data(index_name)
            
            print(f"Successfully migrated index {index_name}")
            
        except Exception as e:
            print(f"Error during migration: {str(e)}")
            if hasattr(e, 'response'):
                print(f"Response content: {e.response.text}")
            raise

def main():
    # Configuration
    source_host = "http://localhost:9200"
    dest_host = "http://192.168.100.243:9200"  # Replace with your remote host
    source_auth = ("elastic", "JumatShubuh04:45")  # Replace with your credentials
    dest_auth = ("elastic", "eL4st1c$!sD@t2024")  # Replace with your credentials
    
    # Indices to migrate
    indices = ["apim_event_faulty", "apim_event_response"]
    
    # Initialize migration
    migration = ElasticsearchMigration(source_host, dest_host, source_auth, dest_auth)
    
    # Migrate each index
    for index in indices:
        try:
            migration.migrate_index(index)
        except Exception as e:
            print(f"Error migrating index {index}: {str(e)}")
            continue

if __name__ == "__main__":
    main()
