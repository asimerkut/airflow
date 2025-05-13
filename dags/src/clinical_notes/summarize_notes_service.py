from .summarize_notes_dao import ClinicalNotesSummarizerDAO
from typing import List, Dict, Any

def get_clinical_note() -> List[Dict[Any, Any]]:
    """
    Retrieves and prints medical notes from the shs_takip table using ClinicalNotesSummarizerDAO.
    
    Args:
        connection_string (str, optional): Database connection string. If None, will use environment variable.
        
    Returns:
        List[Dict[Any, Any]]: List of medical notes from the database
    """
    dao = ClinicalNotesSummarizerDAO()
    notes = dao.get_medical_notes()
    
    print("\n=== Medical Notes ===")
    for i, note in enumerate(notes, 1):
        print(f"\nNote {i}:")
        print("-" * 50)
        print(note['epikriz_aciklama'])
        print("-" * 50)
    
    return notes



