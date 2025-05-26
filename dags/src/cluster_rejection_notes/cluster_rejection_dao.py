from typing import List, Dict, Any, Optional
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
import uuid
import numpy as np
from sqlalchemy.sql import text as sql_text
from ..util.db_util import DatabaseManager

logger = logging.getLogger(__name__)

class ClusterRejectionDAO:
    """
    Data Access Object for cluster rejection operations.
    """
    def __init__(self):
        """
        Initialize the DAO with database connection from DatabaseManager.
        """
        self.db = DatabaseManager()

    def get_rejection_descriptions(self) -> List[Dict[str, Any]]:
        """
        Fetch rejection descriptions from shs_islem table that don't have embeddings yet.
        
        Returns:
            List[Dict[str, Any]]: List of dictionaries containing islem_sira_no and json_kes_aciklama
            for records that don't have embeddings yet
        """
        with self.db.create_session_context() as session:
            query = sql_text("""
                SELECT s.islem_sira_no, s.json_kes_aciklama 
                FROM shs_islem s
                LEFT JOIN kesinti_embedding k ON s.islem_sira_no::text = k.source_id 
                    AND k.embedding_type = 'nomic_v2'
                WHERE s.json_kes_aciklama IS NOT NULL
                AND k.id IS NULL LIMIT 10
            """)
            
            result = session.execute(query)
            return [dict(row) for row in result]

    def insert_rejection_embedding(
        self,
        embedding_type: str,
        text: str,
        nomic_v2_embedding: List[float],
        source_id: int,
        cluster_grp_id: int = None,
        cluster_id: int = None
    ) -> None:
        """
        Insert a new rejection embedding record into kesinti_embedding table.
        
        Args:
            embedding_type (str): Type of the embedding
            text (str): The text content
            nomic_v2_embedding (List[float]): Nomic v2 embedding vector
            source_id (int): Source ID (islem_sira_no)
            cluster_grp_id (int, optional): Cluster group ID
            cluster_id (int, optional): Cluster ID
        """
        # Convert source_id to string for TEXT column
        source_id_str = str(source_id)
        
        with self.db.create_session_context() as session:
            query = sql_text("""
                INSERT INTO kesinti_embedding (
                    id,
                    embedding_type,
                    cluster_grp_id,
                    cluster_id,
                    source_id,
                    text,
                    nomic_v2_embedding
                ) VALUES (
                    gen_random_uuid(),
                    :embedding_type,
                    :cluster_grp_id,
                    :cluster_id,
                    :source_id,
                    :text,
                    CAST(:nomic_v2_embedding AS vector)
                )
            """)
            
            session.execute(query, {
                'embedding_type': embedding_type,
                'cluster_grp_id': cluster_grp_id,
                'cluster_id': cluster_id,
                'source_id': source_id_str,  # Using string version
                'text': text,
                'nomic_v2_embedding': nomic_v2_embedding
            })
            session.commit()

    def check_embedding_exists(self, source_id: int, embedding_type: str) -> bool:
        """
        Check if embedding already exists for given source_id and embedding type.
        
        Args:
            source_id (int): The source ID (islem_sira_no)
            embedding_type (str): Type of the embedding (e.g., 'nomic_v2')
            
        Returns:
            bool: True if embedding exists, False otherwise
        """
        # Convert source_id to string for TEXT column
        source_id_str = str(source_id)
        
        with self.db.create_session_context() as session:
            query = sql_text("""
                SELECT EXISTS (
                    SELECT 1 
                    FROM kesinti_embedding 
                    WHERE source_id = :source_id
                    AND embedding_type = :embedding_type
                )
            """)
            
            result = session.execute(query, {
                'source_id': source_id_str,  # Using string version
                'embedding_type': embedding_type
            }).scalar()
            
            return bool(result)

    def get_all_embeddings(self) -> List[Dict[str, Any]]:
        """
        Fetch all embeddings from the kesinti_embedding table.
        
        Returns:
            List[Dict[str, Any]]: List of dictionaries containing embeddings and metadata
        """
        with self.db.create_session_context() as session:
            query = sql_text("""
                SELECT 
                    source_id, 
                    text, 
                    string_to_array(trim(both '[]' from nomic_v2_embedding::text), ',')::float[] as nomic_v2_embedding
                FROM kesinti_embedding
                WHERE nomic_v2_embedding IS NOT NULL
            """)
            
            result = session.execute(query)
            return [dict(row) for row in result]

    def update_cluster_assignment(self, source_id: int, cluster_id: int) -> None:
        """
        Update the cluster_id for a given source_id in the kesinti_embedding table.
        
        Args:
            source_id (int): The source ID (islem_sira_no)
            cluster_id (int): The cluster ID to assign
        """
        # Convert source_id to string for TEXT column
        source_id_str = str(source_id)
        
        with self.db.create_session_context() as session:
            query = sql_text("""
                UPDATE kesinti_embedding
                SET cluster_id = :cluster_id
                WHERE source_id = :source_id
            """)
            
            session.execute(query, {
                'source_id': source_id_str,
                'cluster_id': cluster_id
            })
            session.commit() 