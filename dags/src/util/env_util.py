from dotenv import load_dotenv
import os
from functools import lru_cache
from airflow.models import Variable
from airflow.exceptions import AirflowException

# Load environment variables when module is imported
load_dotenv()

@lru_cache(maxsize=32)
def get(key: str, default: str = None) -> str:
    """
    Get value from environment variables or Airflow Variables.
    First tries environment variables, then Airflow Variables.
    Raises error if value is not found and no default is provided.
    
    Args:
        key: Variable name
        default: Default value if variable is not found (optional)
        
    Returns:
        str: Variable value or default value
        
    Raises:
        AirflowException: If variable is not found and no default is provided
    """
    # First try environment variable
    value = os.getenv(key)
    if value is not None:
        return value
        
    # Then try Airflow Variable
    try:
        value = Variable.get(key)
        if value is not None:
            return value
    except KeyError:
        pass
        
    # If we have a default value, return it
    if default is not None:
        return default
        
    # If we get here, the variable was not found and no default was provided
    raise AirflowException(f"Required variable '{key}' not found in environment or Airflow Variables")

# Database specific getters
def get_db_host() -> str:
    return get('DATAML_DB_HOST', 'localhost')

def get_db_port() -> str:
    return get('DATAML_DB_PORT', '5432')

def get_db_name() -> str:
    return get('DATAML_DB_NAME', 'medscan')

def get_db_user() -> str:
    return get('DATAML_DB_USER', 'medscan_user')

def get_db_password() -> str:
    return get('DATAML_DB_PASSWORD', '')

def get_db_connection_string() -> str:
    """Get database connection string from environment variables."""
    return f"postgresql://{get_db_user()}:{get_db_password()}@{get_db_host()}:{get_db_port()}/{get_db_name()}"

# Ollama specific getters
def get_ollama_base_url() -> str:
    """Get Ollama base URL from environment variables."""
    return get('OLLAMA_BASE_URL', 'http://host.minikube.internal:11434')

def get_ollama_model() -> str:
    """Get Ollama model name from environment variables."""
    return get('OLLAMA_MODEL', 'gemma3:27b') 