from .summarize_notes_dao import ClinicalNotesSummarizerDAO
from typing import List, Dict, Any
from langchain.llms import Ollama
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from ..util.env_util import get_ollama_base_url, get_ollama_model
from ..util.db_util import DatabaseManager
from sqlalchemy import text
import json
import re

def parse_soap_sections(summary_text: str) -> Dict[str, str]:
    """
    Parse the SOAP sections from a summary text.
    
    Args:
        summary_text (str): The summary text with SOAP sections
        
    Returns:
        Dict[str, str]: Dictionary with 'S', 'O', 'A', 'P' keys and their content
    """
    sections = {
        'S': '',
        'O': '',
        'A': '',
        'P': ''
    }
    
    # Clean up asterisks from the text
    summary_text = summary_text.replace('*', '')
    
    # Simple approach: split by section headers
    lines = summary_text.split('\n')
    current_section = None
    
    for line in lines:
        line = line.strip()
        if "SUBJECTIVE:" in line:
            current_section = 'S'
            # Remove the header from the content
            content = line.replace("SUBJECTIVE:", "").strip()
            if content:
                sections[current_section] += content + "\n"
        elif "OBJECTIVE:" in line:
            current_section = 'O'
            # Remove the header from the content
            content = line.replace("OBJECTIVE:", "").strip()
            if content:
                sections[current_section] += content + "\n"
        elif "ASSESSMENT:" in line:
            current_section = 'A'
            # Remove the header from the content
            content = line.replace("ASSESSMENT:", "").strip()
            if content:
                sections[current_section] += content + "\n"
        elif "PLAN:" in line:
            current_section = 'P'
            # Remove the header from the content
            content = line.replace("PLAN:", "").strip()
            if content:
                sections[current_section] += content + "\n"
        elif current_section and line:
            sections[current_section] += line + "\n"
    
    # Trim trailing newlines
    for key in sections:
        sections[key] = sections[key].strip()
    
    return sections

def summarize() -> None:
    """
    Retrieves medical notes from the database, summarizes them using Ollama LLM,
    and saves each summary immediately after processing.
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
- If a section has no relevant information, write NULL for that section
- Do not make up information
- If the entire note is empty or has no useful information, return NULL for all sections

Note: {note}"""
    )
    
    # Create chain
    chain = LLMChain(llm=llm, prompt=prompt_template)
    
    for note in notes:
        try:
            # Get the response and save immediately
            response = chain.invoke({"note": note['epikriz_aciklama']})
            soap_sections = parse_soap_sections(response['text'])
            dao.save_note_summary(note['takip_no'], soap_sections)
        except Exception:
            continue



