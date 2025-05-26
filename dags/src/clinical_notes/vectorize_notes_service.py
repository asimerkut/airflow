from .summarize_notes_dao import ClinicalNotesSummarizerDAO
from sentence_transformers import SentenceTransformer
import numpy as np
from typing import Dict, Any, List
import logging
import torch

logger = logging.getLogger(__name__)

class ClinicalNotesVectorizer:
    """
    Service for vectorizing clinical notes using BioBERT.
    """
    
    def __init__(self):
        """
        Initialize the vectorizer with BioBERT model and DAO.
        Explicitly set device to CPU.
        """
        self.dao = ClinicalNotesSummarizerDAO()
        # Load BioBERT model and force CPU usage
        self.model = SentenceTransformer('dmis-lab/biobert-base-cased-v1.1', device='cpu')
        logger.info("BioBERT model loaded in CPU mode")
        
    def combine_soap_sections(self, soap_dict: Dict[str, str]) -> str:
        """
        Combine SOAP sections into a single text for vectorization.
        
        Args:
            soap_dict (Dict[str, str]): Dictionary with SOAP sections
            
        Returns:
            str: Combined text
        """
        sections = []
        for section, content in soap_dict.items():
            if content and content.upper() != 'NULL':
                sections.append(f"{section}: {content}")
        return " ".join(sections)
    
    def vectorize_notes(self) -> None:
        """
        Retrieve SOAP summaries, vectorize them using BioBERT,
        and save the vectors to the database.
        """
        # Get summaries that need vectorization
        summaries = self.dao.get_soap_summaries()
        logger.info(f"Found {len(summaries)} summaries to vectorize")
        
        for summary in summaries:
            try:
                # Combine SOAP sections
                soap_dict = {
                    'S': summary.get('ozet_s', ''),
                    'O': summary.get('ozet_o', ''),
                    'A': summary.get('ozet_a', ''),
                    'P': summary.get('ozet_p', '')
                }
                combined_text = self.combine_soap_sections(soap_dict)
                
                # Generate BioBERT vector (explicitly on CPU)
                with torch.no_grad():  # Disable gradient calculation for inference
                    vector = self.model.encode(combined_text, convert_to_numpy=True)
                
                # Save vector to database
                success = self.dao.save_biobert_vector(summary['takip_no'], vector.tolist())
                
                if success:
                    logger.info(f"Successfully vectorized note {summary['takip_no']}")
                else:
                    logger.error(f"Failed to save vector for note {summary['takip_no']}")
                    
            except Exception as e:
                logger.error(f"Error processing note {summary['takip_no']}: {str(e)}")
                continue

def vectorize() -> None:
    """
    Main function to vectorize clinical notes.
    """
    vectorizer = ClinicalNotesVectorizer()
    vectorizer.vectorize_notes() 