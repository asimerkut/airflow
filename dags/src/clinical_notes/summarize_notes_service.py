from .summarize_notes_dao import ClinicalNotesSummarizerDAO
from typing import List, Dict, Any
from langchain.llms import Ollama
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from ..util.env_util import get_ollama_base_url, get_ollama_model
import json
import re

def summarize() -> List[Dict[Any, Any]]:
    """
    Retrieves medical notes from the database and summarizes them using Ollama LLM.
    
    Returns:
        List[Dict[Any, Any]]: List of medical notes with their summaries
    """
    # Initialize DAO and get notes
    dao = ClinicalNotesSummarizerDAO()
    notes = dao.get_medical_notes()
    
    # Initialize Ollama LLM with settings from environment
    llm = Ollama(
        model=get_ollama_model(),
        base_url=get_ollama_base_url()
    )
    
    # Create prompt template for summarization
    prompt_template = PromptTemplate(
        input_variables=["note"],
        template="""Summarize this medical note in English with these 4 sections:

SUBJECTIVE: Patient's symptoms and history
OBJECTIVE: Findings and measurements
ASSESSMENT: Diagnosis
PLAN: Treatment plan

Rules:
- Keep it brief
- Use only information from the text
- Remove personal data
- Leave section empty if no information

Note: {note}"""
    )
    
    # Create chain
    chain = LLMChain(llm=llm, prompt=prompt_template)
    
    print("\n=== Medical Notes and Summaries ===")
    summarized_notes = []
    
    for i, note in enumerate(notes, 1):
        print(f"\nNote {i}:")
        print("-" * 50)
        print("Original Note:")
        print(note['epikriz_aciklama'])
        print("-" * 50)
        
        try:
            # Get the response
            response = chain.invoke({"note": note['epikriz_aciklama']})
            print("Summary:")
            print(response['text'])
            print("-" * 50)
            
            # Add summary to the note dictionary
            note['summary'] = response['text']
            summarized_notes.append(note)
            
        except Exception as e:
            print(f"Error processing note {i}: {str(e)}")
            continue
    
    return summarized_notes



