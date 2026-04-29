"""
Database helper utilities for PostgreSQL connection
Supports both st.secrets and environment variables
"""
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import psycopg2
from typing import Optional

try:
    import streamlit as st
    HAS_STREAMLIT = True
except ImportError:
    HAS_STREAMLIT = False


def get_db_config(key: str, default: str = None) -> str:
    """
    Get database configuration value with priority:
    1. st.secrets (if available and Streamlit is running)
    2. Environment variables
    3. Default value

    Args:
        key: Configuration key ('host', 'port', 'name', 'user', 'password', 'url')
        default: Default value if not found

    Returns:
        Configuration value as string
    """
    # Try Streamlit secrets first (if available)
    if HAS_STREAMLIT:
        try:
            if key == 'host':
                secret_val = st.secrets.get("database", {}).get("host")
                if secret_val:
                    return secret_val
            elif key == 'port':
                secret_val = st.secrets.get("database", {}).get("port")
                if secret_val:
                    return str(secret_val)
            elif key == 'name':
                secret_val = st.secrets.get("database", {}).get("name")
                if secret_val:
                    return secret_val
            elif key == 'user':
                secret_val = st.secrets.get("database", {}).get("user")
                if secret_val:
                    return secret_val
            elif key == 'password':
                secret_val = st.secrets.get("database", {}).get("password")
                if secret_val:
                    return secret_val
            elif key == 'url':
                secret_val = st.secrets.get("database", {}).get("url")
                if secret_val:
                    return secret_val
        except Exception:
            # Secrets not available or error accessing them, fallback to env vars
            pass

    # Fallback to environment variables
    if key == 'host':
        return os.getenv('POSTGRES_HOST', default or 'localhost')
    elif key == 'port':
        return os.getenv('POSTGRES_PORT', default or '5433')
    elif key == 'name':
        return os.getenv('POSTGRES_DB', os.getenv('STREAMLIT_DB_NAME', default or 'flott_streamlit'))
    elif key == 'user':
        return os.getenv('POSTGRES_USER', default or 'postgres')
    elif key == 'password':
        return os.getenv('POSTGRES_PASSWORD', os.getenv('DATABASE_PASSWORD', default or 'postgres'))
    elif key == 'url':
        return os.getenv('DATABASE_URL', default)

    return default


def get_database_url() -> str:
    """
    Get database URL from st.secrets or environment variables

    Priority:
    1. st.secrets['database']['url'] (if provided)
    2. Built from st.secrets['database'] components
    3. Environment variable DATABASE_URL
    4. Built from environment variables
    5. Default values
    """
    # Check if full DATABASE_URL is provided
    db_url = get_db_config('url')
    if db_url:
        return db_url

    # Otherwise build from individual components
    user = get_db_config('user', 'postgres')
    password = get_db_config('password', 'postgres')
    host = get_db_config('host', 'localhost')
    port = get_db_config('port', '5433')
    database = get_db_config('name', 'flott_streamlit')

    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def get_db_engine():
    """Create SQLAlchemy engine"""
    database_url = get_database_url()
    host = get_db_config('host', 'localhost')
    if host != 'localhost':
        return create_engine(database_url, pool_pre_ping=True, connect_args={'sslmode': 'require'})
    return create_engine(database_url, pool_pre_ping=True)


def get_db_session():
    """Get database session"""
    engine = get_db_engine()
    Session = sessionmaker(bind=engine)
    return Session()


def get_db_connection():
    """
    Get raw psycopg2 connection
    Uses st.secrets if available, otherwise falls back to environment variables
    """
    host = get_db_config('host', 'localhost')
    sslmode = 'require' if host != 'localhost' else 'prefer'
    return psycopg2.connect(
        host=host,
        port=get_db_config('port', '5432'),
        database=get_db_config('name', 'flott_streamlit'),
        user=get_db_config('user', 'postgres'),
        password=get_db_config('password', 'postgres'),
        sslmode=sslmode
    )


def test_connection() -> tuple[bool, Optional[str]]:
    """
    Test database connection
    Returns: (success: bool, error_message: Optional[str])
    """
    try:
        conn = get_db_connection()
        conn.close()
        return True, None
    except Exception as e:
        return False, str(e)
