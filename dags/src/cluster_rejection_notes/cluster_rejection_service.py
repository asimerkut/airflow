from typing import List, Dict, Any, Tuple
import logging
import numpy as np
import torch
from langchain_huggingface import HuggingFaceEmbeddings
from sklearn.cluster import KMeans
from collections import defaultdict
from .cluster_rejection_dao import ClusterRejectionDAO

logger = logging.getLogger(__name__)

class ClusterManager:
    def __init__(self):
        pass

    def find_optimal_clusters(self, embeddings_array):
        min_clusters = 3
        max_clusters = min(30, len(embeddings_array) // 5)
        
        if len(embeddings_array) < min_clusters:
            return min_clusters
        
        distortions = []
        K = range(min_clusters, max_clusters + 1)
        
        for k in K:
            kmeans = KMeans(n_clusters=k, random_state=42)
            kmeans.fit(embeddings_array)
            distortions.append(kmeans.inertia_)
        
        distortions = np.array(distortions)
        normalized_distortions = (distortions - np.min(distortions)) / (np.max(distortions) - np.min(distortions))
        
        changes = np.diff(normalized_distortions)
        threshold = np.mean(np.abs(changes)) * 0.5
        optimal_idx = np.where(np.abs(changes) < threshold)[0][0]
        
        return optimal_idx + min_clusters

    def perform_clustering(self, embeddings_array, texts):
        kmeans = KMeans(n_clusters=min(10, len(texts)), random_state=42)
        clusters = kmeans.fit_predict(embeddings_array)
        
        grouped_texts = {}
        for idx, cluster_id in enumerate(clusters):
            if cluster_id not in grouped_texts:
                grouped_texts[cluster_id] = []
            grouped_texts[cluster_id].append(texts[idx])
            
        return grouped_texts, len(set(clusters))

class ClusterRejectionService:
    """
    Service layer for cluster rejection operations.
    """
    def __init__(self):
        self.dao = ClusterRejectionDAO()
        self.cluster_manager = ClusterManager()
        
        # Check if CUDA is available and set device accordingly
        device = 'cuda' if torch.cuda.is_available() else 'cpu'
        if device == 'cuda':
            logger.info(f"Using GPU: {torch.cuda.get_device_name(0)}")
        else:
            logger.info("CUDA not available, using CPU")
        
        # Initialize Nomic v2 embedding model with appropriate device
        self.model = HuggingFaceEmbeddings(
            model_name="nomic-ai/nomic-embed-text-v2-moe",
            model_kwargs={'trust_remote_code': True, 'device': device},
            encode_kwargs={'normalize_embeddings': True}
        )
        logger.info(f"Nomic v2 model initialized in {device.upper()} mode")

    def create_rejection_embedding(self) -> None:
        """
        Create embeddings for rejection descriptions.
        1. Fetch rejection descriptions from database
        2. Generate embeddings for each description (if not already exists)
        3. Save embeddings to database
        """
        try:
            # Get rejection descriptions from database
            rejections = self.dao.get_rejection_descriptions()
            logger.info(f"Fetched {len(rejections)} rejection descriptions")
            
            # Process each rejection and create embeddings
            for rejection in rejections:
                try:
                    text = rejection['json_kes_aciklama']
                    if not text or not isinstance(text, str):
                        logger.warning(f"Skipping invalid text: {text}")
                        continue
                    
                    # Get islem_sira_no for source_id
                    source_id = rejection.get('islem_sira_no')
                    if not source_id:
                        logger.warning(f"Skipping record with no source_id")
                        continue

                    # Check if embedding already exists
                    embedding_type = "nomic_v2"
                    if self.dao.check_embedding_exists(source_id, embedding_type):
                        logger.info(f"Embedding already exists for source_id {source_id}, skipping...")
                        continue
                    
                    # Generate embedding using Nomic v2 in CPU mode
                    with torch.no_grad():  # Disable gradient calculation for inference
                        # embed_documents returns a list, we take the first (and only) element
                        embedding = np.array(self.model.embed_documents([text])[0], dtype=np.float32)
                        # Convert numpy array to Python list for database storage
                        embedding_list = embedding.tolist()
                    
                    # Save to database
                    self.dao.insert_rejection_embedding(
                        embedding_type=embedding_type,
                        text=text,
                        nomic_v2_embedding=embedding_list,  # Convert to list before passing
                        source_id=source_id
                    )
                    
                    logger.info(f"Successfully processed and saved embedding for source_id {source_id}")
                    
                except Exception as e:
                    logger.error(f"Error processing individual rejection: {str(e)}")
                    continue  # Continue with next rejection even if one fails
            
            logger.info("Finished processing all rejections")
            
        except Exception as e:
            logger.error(f"Error in embedding creation process: {str(e)}")
            raise

    def perform_clustering_and_update(self) -> None:
        """
        Perform clustering on existing embeddings and update cluster assignments in the database.
        1. Fetch all embeddings from database
        2. Perform clustering using KMeans
        3. Update database with cluster assignments
        """
        try:
            # Fetch all embeddings from database
            embeddings_data = self.dao.get_all_embeddings()
            if not embeddings_data:
                logger.warning("No embeddings found in database")
                return

            # Prepare data for clustering
            embeddings_array = []
            source_ids = []
            texts = []
            
            for record in embeddings_data:
                try:
                    if record.get('nomic_v2_embedding'):
                        # The embedding is already a list of floats from the database
                        embeddings_array.append(record['nomic_v2_embedding'])
                        source_ids.append(record['source_id'])
                        texts.append(record['text'])
                except Exception as e:
                    logger.error(f"Error processing embedding for source_id {record.get('source_id')}: {str(e)}")
                    continue

            if not embeddings_array:
                logger.warning("No valid embeddings found for clustering")
                return

            # Convert to numpy array
            embeddings_array = np.array(embeddings_array)
            logger.info(f"Successfully prepared {len(embeddings_array)} embeddings for clustering")
            
            # Find optimal number of clusters
            optimal_clusters = self.cluster_manager.find_optimal_clusters(embeddings_array)
            logger.info(f"Optimal number of clusters determined: {optimal_clusters}")

            # Perform clustering
            kmeans = KMeans(n_clusters=optimal_clusters, random_state=42)
            cluster_assignments = kmeans.fit_predict(embeddings_array)

            # Update database with cluster assignments
            for source_id, cluster_id in zip(source_ids, cluster_assignments):
                try:
                    self.dao.update_cluster_assignment(
                        source_id=source_id,
                        cluster_id=int(cluster_id)
                    )
                    logger.info(f"Updated cluster assignment for source_id {source_id} to cluster {cluster_id}")
                except Exception as e:
                    logger.error(f"Error updating cluster assignment for source_id {source_id}: {str(e)}")
                    continue

            # Log clustering statistics
            cluster_sizes = defaultdict(int)
            for cluster_id in cluster_assignments:
                cluster_sizes[cluster_id] += 1
            
            logger.info("Clustering completed successfully")
            logger.info(f"Total clusters: {optimal_clusters}")
            logger.info("Cluster sizes:")
            for cluster_id, size in sorted(cluster_sizes.items()):
                logger.info(f"Cluster {cluster_id}: {size} items")

        except Exception as e:
            logger.error(f"Error in clustering process: {str(e)}")
            raise

def create_embeddings() -> None:
    """
    Main function to create embeddings for transaction rejections.
    """
    service = ClusterRejectionService()
    service.create_rejection_embedding()

def perform_clustering() -> None:
    """
    Main function to perform clustering on existing embeddings.
    """
    service = ClusterRejectionService()
    service.perform_clustering_and_update() 