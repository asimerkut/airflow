from .summarize_notes_dao import ClinicalNotesSummarizerDAO
from typing import List, Dict, Any
from langchain.llms import Ollama
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain

def summarize() -> List[Dict[Any, Any]]:
    """
    Retrieves medical notes from the database and summarizes them using Ollama LLM.
    
    Returns:
        List[Dict[Any, Any]]: List of medical notes with their summaries
    """
    # Initialize DAO and get notes
    dao = ClinicalNotesSummarizerDAO()
    notes = dao.get_medical_notes()
    
    # Initialize Ollama LLM
    llm = Ollama(
        model="gemma3:27b",
        base_url="http://localhost:11434"
    )
    
    # Create prompt template for summarization
    prompt_template = PromptTemplate(
        input_variables=["note"],
        template="""Please provide a very concise summary of the following medical note in Turkish. 
        Focus only on the most critical medical information.
        Maximum 2-3 sentences.
        Remove all personal information.
        Format: Just the summary, no explanations or translations.

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
        
        # Generate summary
        summary = chain.run(note['epikriz_aciklama'])
        
        print("Summary:")
        print(summary)
        print("-" * 50)
        
        # Add summary to the note dictionary
        note['summary'] = summary
        summarized_notes.append(note)
    
    return summarized_notes



