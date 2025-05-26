import sys
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

# Add the src directory to Python path so we can import our modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.cluster_rejection_notes.cluster_rejection_service import create_embeddings, perform_clustering

def test_create_embeddings():
    """Test function that runs the embedding creation service"""
    create_embeddings()

def test_perform_clustering():
    """Test function that runs the clustering service"""
    perform_clustering()

if __name__ == '__main__':
    # Run both services in sequence
    test_create_embeddings()
    test_perform_clustering() 