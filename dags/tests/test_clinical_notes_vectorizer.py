import sys
import os

# Add the src directory to Python path so we can import our modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.clinical_notes.vectorize_notes_service import vectorize

def test_vectorize():
    """Test function that runs the vectorizer"""
    vectorize()

if __name__ == '__main__':
    # Just run the vectorizer
    vectorize()

