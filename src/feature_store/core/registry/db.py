from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from feature_store.config import settings
from feature_store.core.registry.models import Base

# Create the engine
engine = create_engine(settings.database_url, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    """Create tables if they don't exist"""
    settings.make_dirs() # Ensure folders exist
    Base.metadata.create_all(bind=engine)

def get_db():
    """Dependency for yielding database sessions"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()