#!/usr/bin/env python3
"""
Elasticsearch Index Copier

This script facilitates copying Elasticsearch indices between clusters,
including settings, mappings, and data. It uses the scroll API for efficient
data transfer and supports batch processing.
"""

import os
import sys
import json
import logging
from datetime import datetime
import argparse
from typing import Tuple, Dict, List, Optional
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('copier.log')
    ]
)
logger = logging.getLogger(__name__)

class CopyStatistics:
    """Track statistics for the copy operation."""
    
    def __init__(self):
        self.successful_copies = []  # List of (source, target, doc_count)
        self.failed_copies = []      # List of (source, target, error)
        self.total_documents = 0
        self.start_time = None
        self.end_time = None

    def add_success(self, source: str, target: str, doc_count: int):
        """Record a successful copy operation."""
        self.successful_copies.append((source, target, doc_count))
        self.total_documents += doc_count

    def add_failure(self, source: str, target: str, error: str):
        """Record a failed copy operation."""
        self.failed_copies.append((source, target, error))

    def start(self):
        """Mark the start time of the copy operation."""
        self.start_time = datetime.now()

    def finish(self):
        """Mark the end time of the copy operation."""
        self.end_time = datetime.now()

    def summary(self) -> str:
        """Generate a summary of the copy operation."""
        duration = self.end_time - self.start_time
        success_count = len(self.successful_copies)
        fail_count = len(self.failed_copies)
        
        summary = [
            "\nCopy Operation Summary:",
            f"Duration: {duration}",
            f"Total Documents Copied: {self.total_documents:,}",
            f"Successful Copies: {success_count}",
            f"Failed Copies: {fail_count}",
        ]
        
        if self.successful_copies:
            summary.append("\nSuccessful Operations:")
            for source, target, count in self.successful_copies:
                summary.append(f"  - {source} → {target} ({count:,} documents)")
        
        if self.failed_copies:
            summary.append("\nFailed Operations:")
            for source, target, error in self.failed_copies:
                summary.append(f"  - {source} → {target}")
                summary.append(f"    Error: {error}")
        
        return "\n".join(summary)

class ElasticsearchCopier:
    """Handles the copying of Elasticsearch indices between clusters."""

    def __init__(self, source_host: str, dest_host: str, source_auth: Tuple[str, str], 
                 dest_auth: Tuple[str, str], batch_size: int = 1000,
                 total_fields_limit: Optional[int] = None):
        """
        Initialize the copier handler.

        Args:
            source_host: Source Elasticsearch cluster URL
            dest_host: Destination Elasticsearch cluster URL
            source_auth: Tuple of (username, password) for source cluster
            dest_auth: Tuple of (username, password) for destination cluster
            batch_size: Number of documents to process in each batch
            total_fields_limit: Maximum number of fields in mappings (-1 to use source setting)
        """
        self.source_host = source_host.rstrip('/')
        self.dest_host = dest_host.rstrip('/')
        self.source_auth = HTTPBasicAuth(*source_auth)
        self.dest_auth = HTTPBasicAuth(*dest_auth)
        self.headers = {'Content-Type': 'application/json'}
        self.batch_size = batch_size
        self.total_fields_limit = total_fields_limit
        self.stats = CopyStatistics()

    def get_source_index_info(self, index_name: str) -> Tuple[Dict, Dict]:
        """
        Get settings and mappings from source index.

        Args:
            index_name: Name of the index to get information from

        Returns:
            Tuple of (settings, mappings) dictionaries
        """
        try:
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
            
            logger.info(f"Retrieved settings and mappings for index: {index_name}")
            return settings, mappings
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting index info: {str(e)}")
            raise

    def create_index_with_settings(self, source_index: str, target_index: str, 
                                 source_settings: Dict, source_mappings: Dict) -> Dict:
        """
        Create index with source settings and mappings.

        Args:
            source_index: Name of the source index
            target_index: Name of the target index (can be different from source)
            source_settings: Settings from source index
            source_mappings: Mappings from source index

        Returns:
            Response from Elasticsearch
        """
        url = f"{self.dest_host}/{target_index}"
        
        # Delete existing index if it exists
        try:
            requests.delete(url, auth=self.dest_auth)
            logger.info(f"Deleted existing index: {target_index}")
        except requests.exceptions.RequestException:
            pass

        # Get total fields limit from source if configured
        source_total_fields = source_settings.get("index", {}).get("mapping", {}).get(
            "total_fields.limit")
        
        # Create settings
        settings = {
            "index": {
                "number_of_shards": source_settings.get("index", {}).get("number_of_shards", "1"),
                "number_of_replicas": source_settings.get("index", {}).get("number_of_replicas", "1")
            }
        }

        # Add total_fields_limit only if it's a positive number or if using source setting
        if self.total_fields_limit == -1:
            if source_total_fields:
                settings["index"]["mapping"] = {
                    "total_fields": {
                        "limit": source_total_fields
                    }
                }
        elif self.total_fields_limit and self.total_fields_limit > 0:
            settings["index"]["mapping"] = {
                "total_fields": {
                    "limit": self.total_fields_limit
                }
            }
        
        config = {
            "settings": settings,
            "mappings": source_mappings
        }
        
        try:
            response = requests.put(url, auth=self.dest_auth, headers=self.headers, 
                                  json=config)
            response.raise_for_status()
            logger.info(f"Created index {target_index} with settings and mappings")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error creating index: {str(e)}")
            raise

    def reindex_data(self, source_index: str, target_index: str) -> int:
        """
        Reindex data using scroll API.

        Args:
            source_index: Name of the source index
            target_index: Name of the target index

        Returns:
            Total number of documents copied
        """
        print(f"Copying documents from {source_index} to {target_index}")
        scroll_url = f"{self.source_host}/{source_index}/_search?scroll=5m"
        scroll_body = {
            "size": self.batch_size,
            "query": {"match_all": {}}
        }
        
        try:
            response = requests.post(scroll_url, auth=self.source_auth, 
                                   headers=self.headers, json=scroll_body)
            response.raise_for_status()
            
            scroll_id = response.json()['_scroll_id']
            hits = response.json()['hits']['hits']
            total_docs = 0
            
            while hits:
                bulk_body = ""
                for hit in hits:
                    action = {"index": {"_index": target_index, "_id": hit['_id']}}
                    bulk_body += json.dumps(action) + "\n"
                    bulk_body += json.dumps(hit['_source']) + "\n"
                
                bulk_url = f"{self.dest_host}/_bulk"
                response = requests.post(bulk_url, auth=self.dest_auth, 
                                      headers=self.headers, data=bulk_body)
                
                if response.status_code != 200:
                    logger.error(f"Error in bulk operation: {response.text}")
                else:
                    resp_json = response.json()
                    if resp_json.get('errors', False):
                        logger.warning("Bulk operation had errors:", 
                                     json.dumps(resp_json, indent=2))
                
                total_docs += len(hits)
                print(f"Progress: {total_docs:,} documents copied", end='\r')
                
                scroll_url = f"{self.source_host}/_search/scroll"
                scroll_body = {
                    "scroll": "5m",
                    "scroll_id": scroll_id
                }
                response = requests.post(scroll_url, auth=self.source_auth, 
                                      headers=self.headers, json=scroll_body)
                response.raise_for_status()
                
                scroll_id = response.json()['_scroll_id']
                hits = response.json()['hits']['hits']
            
            # Clean up scroll
            requests.delete(f"{self.source_host}/_search/scroll", 
                          auth=self.source_auth, 
                          headers=self.headers, 
                          json={"scroll_id": scroll_id})
            
            print(f"\nCompleted copying {total_docs:,} documents")
            return total_docs
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error during reindexing: {str(e)}")
            raise

    def copy_index(self, source_index: str, target_index: str) -> None:
        """
        Copy index with settings.

        Args:
            source_index: Name of the source index
            target_index: Name of the target index (can be different from source)
        """
        print(f"\nCopying index: {source_index} → {target_index}")
        try:
            settings, mappings = self.get_source_index_info(source_index)
            self.create_index_with_settings(source_index, target_index, settings, mappings)
            total_docs = self.reindex_data(source_index, target_index)
            print(f"Successfully copied index {source_index} to {target_index}")
            self.stats.add_success(source_index, target_index, total_docs)
            
        except Exception as e:
            error_msg = str(e)
            if hasattr(e, 'response'):
                error_msg += f"\nResponse: {e.response.text}"
            self.stats.add_failure(source_index, target_index, error_msg)
            logger.error(f"Error during copying: {error_msg}")
            raise

def parse_index_mapping(indices_str: str) -> Dict[str, str]:
    """Parse index mapping string into source->target dictionary."""
    if not indices_str:
        return {}
    
    index_map = {}
    for index_pair in indices_str.split(','):
        if ':' in index_pair:
            source, target = index_pair.strip().split(':')
            index_map[source.strip()] = target.strip()
        else:
            index_map[index_pair.strip()] = index_pair.strip()
    return index_map

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Copy Elasticsearch indices between clusters',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '--indices',
        help='List of indices to copy (format: source1:target1,source2:target2)',
        default=None
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        help='Number of documents to process in each batch',
        default=None
    )
    parser.add_argument(
        '--total-fields-limit',
        type=int,
        help='Maximum number of fields in mappings (-1 to use source setting)',
        default=None
    )
    parser.add_argument(
        '-v', '--verbose',
        action='count',
        default=0,
        help='Increase verbosity level (use -vv for debug)'
    )
    return parser.parse_args()

def setup_logging(verbosity: int) -> None:
    """Configure logging based on verbosity level."""
    if verbosity >= 2:
        level = logging.DEBUG
    elif verbosity == 1:
        level = logging.INFO
    else:
        level = logging.WARNING
    
    logging.getLogger().setLevel(level)

def main() -> None:
    """Main entry point for the copier tool."""
    # Parse arguments first to setup logging
    args = parse_args()
    setup_logging(args.verbose)
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment
    source_host = os.getenv('SOURCE_HOST', 'http://localhost:9200')
    dest_host = os.getenv('DEST_HOST')
    source_auth = (
        os.getenv('SOURCE_USERNAME', 'elastic'),
        os.getenv('SOURCE_PASSWORD')
    )
    dest_auth = (
        os.getenv('DEST_USERNAME', 'elastic'),
        os.getenv('DEST_PASSWORD')
    )
    
    # Get indices from arguments or environment
    indices_str = args.indices or os.getenv('INDICES')
    if not indices_str:
        logger.error("No indices specified. Use --indices argument or INDICES environment variable.")
        sys.exit(1)
    
    # Parse index mappings
    index_map = parse_index_mapping(indices_str)
    if not index_map:
        logger.error("No valid index mappings found.")
        sys.exit(1)
    
    # Get other settings
    batch_size = args.batch_size or int(os.getenv('BATCH_SIZE', '1000'))
    total_fields_limit = args.total_fields_limit or int(os.getenv('TOTAL_FIELDS_LIMIT', '2000'))
    
    # Validate required environment variables
    if not all([dest_host, source_auth[1], dest_auth[1]]):
        logger.error("Missing required environment variables. Please check .env file.")
        sys.exit(1)
    
    # Initialize copier
    copier = ElasticsearchCopier(
        source_host, 
        dest_host, 
        source_auth, 
        dest_auth,
        batch_size,
        total_fields_limit
    )
    
    # Start statistics tracking
    copier.stats.start()
    total_indices = len(index_map)
    print(f"\nStarting copy operation for {total_indices} indices...")
    
    # Copy each index
    for source_index, target_index in index_map.items():
        try:
            copier.copy_index(source_index, target_index)
        except Exception as e:
            logger.error(f"Error copying index {source_index} to {target_index}: {str(e)}")
            continue
    
    # Show final statistics
    copier.stats.finish()
    print(copier.stats.summary())

if __name__ == "__main__":
    main()
