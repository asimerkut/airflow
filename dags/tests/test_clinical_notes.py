import unittest
import sys
import os

# Add the src directory to Python path so we can import our modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.clinical_notes.summarize_notes_service import get_clinical_note

class TestClinicalNotes(unittest.TestCase):
    """
    Test cases for clinical notes service.
    These tests use real database connection - no mocking.
    """
    
    def test_get_clinical_notes_returns_list(self):
        """Test that get_clinical_note returns a list"""
        notes = get_clinical_note()
        self.assertIsInstance(notes, list)
    
    def test_get_clinical_notes_has_content(self):
        """Test that returned notes have epikriz_aciklama field"""
        notes = get_clinical_note()
        self.assertTrue(len(notes) > 0, "Should return at least one note")
        
        for note in notes:
            self.assertIn('epikriz_aciklama', note)
            self.assertIsNotNone(note['epikriz_aciklama'])
            self.assertIsInstance(note['epikriz_aciklama'], str)

if __name__ == '__main__':
    unittest.main() 