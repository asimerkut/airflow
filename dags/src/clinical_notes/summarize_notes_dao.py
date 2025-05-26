from sqlalchemy import text
from typing import List, Dict, Any
from ..util.db_util import DatabaseManager

class ClinicalNotesSummarizerDAO:
    """
    Data Access Object for clinical notes summarization operations.
    Handles database interactions for retrieving medical notes and saving summaries.
    """
    
    def __init__(self):
        """
        Initialize the DAO with database connection from DatabaseManager.
        """
        self.db = DatabaseManager()
    
    def get_medical_notes(self) -> List[Dict[Any, Any]]:
        """
        Retrieve medical notes (epikriz_aciklama) from the shs_takip table.
        Excludes records that already have summaries in epikriz_ozet table.
        
        Returns:
            List[Dict[Any, Any]]: List of medical notes, limited to 10 records
        """
        with self.db.create_session_context() as session:
            query = text("""
                SELECT s.takip_no, s.epikriz_aciklama 
                FROM shs_takip s
                LEFT JOIN epikriz_ozet e ON s.takip_no = e.takip_no
                WHERE s.epikriz_aciklama IS NOT NULL
                AND e.takip_no IS NULL
                LIMIT 10
            """)
            
            result = session.execute(query)
            return [dict(row) for row in result]
            
    def save_note_summary(self, takip_no: str, soap_sections: Dict[str, str]) -> bool:
        """
        Save a single note summary to the epikriz_ozet table.
        Only saves non-NULL sections.
        
        Args:
            takip_no (str): The takip number of the note
            soap_sections (Dict[str, str]): Dictionary with 'S', 'O', 'A', 'P' keys and their content
            
        Returns:
            bool: True if save was successful, False otherwise
        """
        try:
            # Filter out NULL values
            non_null_sections = {
                k: v for k, v in soap_sections.items() 
                if v and v.upper() != 'NULL'
            }
            
            if not non_null_sections:
                return False
            
            with self.db.create_session_context() as session:
                # Build the column names and values for the INSERT part
                columns = ['takip_no'] + [f'ozet_{k.lower()}' for k in non_null_sections.keys()]
                values = [':takip_no'] + [f':ozet_{k.lower()}' for k in non_null_sections.keys()]
                
                # Build the SET part for the UPDATE
                update_fields = [f"ozet_{k.lower()} = :ozet_{k.lower()}" for k in non_null_sections.keys()]
                
                # Prepare parameters
                params = {'takip_no': takip_no}
                params.update({f'ozet_{k.lower()}': v for k, v in non_null_sections.items()})
                
                # Prepare SQL query for upsert operation
                query = text(f"""
                    INSERT INTO public.epikriz_ozet
                    ({', '.join(columns)})
                    VALUES ({', '.join(values)})
                    ON CONFLICT (takip_no) 
                    DO UPDATE SET
                        {', '.join(update_fields)}
                """)
                
                # Execute the query
                session.execute(query, params)
                session.commit()
                return True
                
        except Exception as e:
            return False 