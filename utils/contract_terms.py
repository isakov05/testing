"""
Contract Payment Terms Management

Functions for loading and saving contract payment terms from/to database.
"""

import pandas as pd
from typing import Dict, Optional
from utils.db_operations import get_db_connection


def load_contract_payment_terms(user_id: str) -> Dict[str, int]:
    """
    Load contract payment terms from database for a user.
    
    Args:
        user_id: User identifier
        
    Returns:
        Dictionary mapping contract_number to payment_days
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        query = """
            SELECT contract_number, payment_days
            FROM contract_payment_terms
            WHERE user_id = %s
            ORDER BY contract_number
        """
        
        cur.execute(query, (str(user_id),))
        rows = cur.fetchall()
        
        # Convert to dictionary
        terms = {row[0]: row[1] for row in rows}
        
        return terms
        
    except Exception as e:
        print(f"Error loading contract payment terms: {e}")
        # Return empty dict if table doesn't exist yet or other error
        return {}
    finally:
        if conn:
            conn.close()


def save_contract_payment_term(
    user_id: str,
    contract_number: str,
    payment_days: int,
    created_by: Optional[str] = None
) -> bool:
    """
    Save or update a contract payment term in database.
    
    Args:
        user_id: User identifier
        contract_number: Contract number
        payment_days: Number of days from invoice date until due
        created_by: Username of who created/updated this
        
    Returns:
        True if successful, False otherwise
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        query = """
            INSERT INTO contract_payment_terms (user_id, contract_number, payment_days, created_by, updated_at)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id, contract_number)
            DO UPDATE SET 
                payment_days = EXCLUDED.payment_days,
                updated_at = CURRENT_TIMESTAMP
        """
        
        cur.execute(query, (str(user_id), str(contract_number), int(payment_days), str(created_by or user_id)))
        conn.commit()
        
        return True
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error saving contract payment term: {e}")
        return False
    finally:
        if conn:
            conn.close()


def delete_contract_payment_term(user_id: str, contract_number: str) -> bool:
    """
    Delete a contract payment term from database.
    
    Args:
        user_id: User identifier
        contract_number: Contract number to delete
        
    Returns:
        True if successful, False otherwise
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        query = """
            DELETE FROM contract_payment_terms
            WHERE user_id = %s AND contract_number = %s
        """
        
        cur.execute(query, (str(user_id), str(contract_number)))
        conn.commit()
        
        return True
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error deleting contract payment term: {e}")
        return False
    finally:
        if conn:
            conn.close()


def save_all_contract_terms(user_id: str, terms_dict: Dict[str, int], created_by: Optional[str] = None) -> bool:
    """
    Save multiple contract payment terms at once.
    
    Args:
        user_id: User identifier
        terms_dict: Dictionary of {contract_number: payment_days}
        created_by: Username of who saved these
        
    Returns:
        True if all successful, False otherwise
    """
    success = True
    for contract, days in terms_dict.items():
        if not save_contract_payment_term(user_id, contract, days, created_by):
            success = False
    
    return success

