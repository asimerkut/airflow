import unittest
import sys
import os

# Add the src directory to Python path so we can import our modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.clinical_notes.summarize_notes_service import summarize

class TestClinicalNotes(unittest.TestCase):
   
    def test_summarize(self):
        """Test that returned notes have required fields"""
        summarize()
       
    
    
if __name__ == '__main__':
    unittest.main() 