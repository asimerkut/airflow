from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager
from .env_util import get_db_connection_string

class DatabaseManager:
    """
    Utility class for managing database connections and sessions.
    """
    
    _instance = None
    _engine = None
    _Session = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._engine is None:
            self._initialize_connection()
    
    def _initialize_connection(self):
        """Initialize database connection using environment variables."""
        self._engine = create_engine(get_db_connection_string())
        self._Session = sessionmaker(bind=self._engine)
    
    @property
    def engine(self):
        return self._engine
    
    def get_session(self) -> Session:
        return self._Session()
    
    @contextmanager
    def create_session_context(self):
        """Create a session context that automatically handles commit/rollback and closing."""
        session = self.get_session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close() 