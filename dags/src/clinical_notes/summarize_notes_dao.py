from sqlalchemy import text
from typing import List, Dict, Any
from ..util.db_util import DatabaseManager

class ClinicalNotesSummarizerDAO:
    """
    Data Access Object for clinical notes summarization operations.
    Handles database interactions for retrieving medical notes.
    """
    
    def __init__(self):
        """
        Initialize the DAO with database connection from DatabaseManager.
        """
        self.db = DatabaseManager()
    
    def get_medical_notes(self) -> List[Dict[Any, Any]]:
        """
        Retrieve medical notes (epikriz_aciklama) from the shs_takip table.
        
        Returns:
            List[Dict[Any, Any]]: List of medical notes, limited to 10 records
        """
        with self.db.create_session_context() as session:
            query = text("""
                SELECT epikriz_aciklama 
                FROM shs_takip 
                LIMIT 1
            """)
            
            result = session.execute(query)
            return [dict(row) for row in result] 