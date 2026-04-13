"""
Database operations for company information (MyOrg integration)
Handles saving and loading company data from PostgreSQL with JSONB support
"""
import pandas as pd
import json
from typing import Optional, Tuple, Dict, Any, List
from datetime import datetime
from .db_helper import get_db_connection, get_db_engine
from .myorg_api import (
    fetch_company_by_inn,
    parse_myorg_response,
    fetch_company_deals,
    fetch_court_cases,
    fetch_company_connections,
    fetch_company_liabilities,
    fetch_company_licenses
)


def save_company_info(company_data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Save or update company information in database
    Stores complete API response in raw_data JSONB column

    Args:
        company_data: Dictionary with company information (includes 'raw_data' key)

    Returns:
        (success, error_message) tuple
    """
    if not company_data or not company_data.get('inn'):
        return False, "Invalid company data - INN is required"

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Check if company info already exists for this INN
        cur.execute("SELECT id FROM company_info WHERE inn = %s", (company_data.get('inn'),))
        existing = cur.fetchone()

        # Extract raw_data for JSONB storage
        raw_data_json = json.dumps(company_data.get('raw_data', {}))

        if existing:
            # Update existing record
            update_query = """
                UPDATE company_info SET
                    company_name = %s,
                    uuid = %s,
                    company_id = %s,
                    status = %s,
                    status_description = %s,
                    activity_state = %s,
                    registration_number = %s,
                    registration_date = %s,
                    registration_center = %s,
                    statutory_fund = %s,
                    director_name = %s,
                    is_small_business = %s,
                    enterprise_category = %s,
                    taxation_type = %s,
                    oked_code = %s,
                    oked_description = %s,
                    opf_code = %s,
                    opf_description = %s,
                    soogu_code = %s,
                    soogu_description = %s,
                    soato_code = %s,
                    soato_description = %s,
                    region = %s,
                    city = %s,
                    street_address = %s,
                    email = %s,
                    phone = %s,
                    village_code = %s,
                    village_name = %s,
                    trust = %s,
                    score = %s,
                    itpark = %s,
                    is_bankrupt = %s,
                    is_abuse_vat = %s,
                    is_large_taxpayer = %s,
                    relevance_date = %s,
                    is_verified = %s,
                    raw_data = %s::jsonb,
                    last_updated = CURRENT_TIMESTAMP
                WHERE inn = %s
            """
            cur.execute(update_query, (
                company_data.get('company_name'),
                company_data.get('uuid'),
                company_data.get('company_id'),
                company_data.get('status'),
                company_data.get('status_description'),
                company_data.get('activity_state'),
                company_data.get('registration_number'),
                company_data.get('registration_date'),
                company_data.get('registration_center'),
                company_data.get('statutory_fund'),
                company_data.get('director_name'),
                company_data.get('is_small_business', False),
                company_data.get('enterprise_category'),
                company_data.get('taxation_type'),
                company_data.get('oked_code'),
                company_data.get('oked_description'),
                company_data.get('opf_code'),
                company_data.get('opf_description'),
                company_data.get('soogu_code'),
                company_data.get('soogu_description'),
                company_data.get('soato_code'),
                company_data.get('soato_description'),
                company_data.get('region'),
                company_data.get('city'),
                company_data.get('street_address'),
                company_data.get('email'),
                company_data.get('phone'),
                company_data.get('village_code'),
                company_data.get('village_name'),
                company_data.get('trust'),
                company_data.get('score'),
                company_data.get('itpark', False),
                company_data.get('is_bankrupt', False),
                company_data.get('is_abuse_vat', False),
                company_data.get('is_large_taxpayer', False),
                company_data.get('relevance_date'),
                company_data.get('is_verified', False),
                raw_data_json,
                company_data.get('inn')
            ))
        else:
            # Insert new record
            insert_query = """
                INSERT INTO company_info (
                    inn, company_name, uuid, company_id, status, status_description, activity_state,
                    registration_number, registration_date, registration_center,
                    statutory_fund, director_name, is_small_business,
                    enterprise_category, taxation_type,
                    oked_code, oked_description,
                    opf_code, opf_description,
                    soogu_code, soogu_description,
                    soato_code, soato_description,
                    region, city, street_address, email, phone,
                    village_code, village_name,
                    trust, score, itpark, is_bankrupt, is_abuse_vat, is_large_taxpayer,
                    relevance_date, is_verified, raw_data
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
                )
            """
            cur.execute(insert_query, (
                company_data.get('inn'),
                company_data.get('company_name'),
                company_data.get('uuid'),
                company_data.get('company_id'),
                company_data.get('status'),
                company_data.get('status_description'),
                company_data.get('activity_state'),
                company_data.get('registration_number'),
                company_data.get('registration_date'),
                company_data.get('registration_center'),
                company_data.get('statutory_fund'),
                company_data.get('director_name'),
                company_data.get('is_small_business', False),
                company_data.get('enterprise_category'),
                company_data.get('taxation_type'),
                company_data.get('oked_code'),
                company_data.get('oked_description'),
                company_data.get('opf_code'),
                company_data.get('opf_description'),
                company_data.get('soogu_code'),
                company_data.get('soogu_description'),
                company_data.get('soato_code'),
                company_data.get('soato_description'),
                company_data.get('region'),
                company_data.get('city'),
                company_data.get('street_address'),
                company_data.get('email'),
                company_data.get('phone'),
                company_data.get('village_code'),
                company_data.get('village_name'),
                company_data.get('trust'),
                company_data.get('score'),
                company_data.get('itpark', False),
                company_data.get('is_bankrupt', False),
                company_data.get('is_abuse_vat', False),
                company_data.get('is_large_taxpayer', False),
                company_data.get('relevance_date'),
                company_data.get('is_verified', False),
                raw_data_json
            ))

        conn.commit()
        return True, None

    except Exception as e:
        if conn:
            conn.rollback()
        error_msg = f"Error saving company info: {str(e)}"
        print(error_msg)
        return False, error_msg

    finally:
        if conn:
            conn.close()


def load_company_info(company_inn: str) -> Optional[Dict[str, Any]]:
    """
    Load company information from database including raw JSONB data

    Args:
        company_inn: Company INN (Tax Identification Number)

    Returns:
        Dictionary with company information or None if not found
    """
    try:
        engine = get_db_engine()

        query = """
            SELECT
                id, inn, company_name, uuid, company_id, status, status_description, activity_state,
                registration_number, registration_date, registration_center,
                statutory_fund, director_name, is_small_business,
                enterprise_category, taxation_type,
                oked_code, oked_description,
                opf_code, opf_description,
                soogu_code, soogu_description,
                soato_code, soato_description,
                region, city, street_address, email, phone,
                village_code, village_name,
                trust, score, itpark, is_bankrupt, is_abuse_vat, is_large_taxpayer,
                relevance_date, is_verified,
                raw_data, last_updated, created_at
            FROM company_info
            WHERE inn = %(company_inn)s
        """

        df = pd.read_sql_query(query, engine, params={'company_inn': company_inn})

        if df.empty:
            return None

        # Convert first row to dictionary
        company_info = df.iloc[0].to_dict()

        # Convert pandas timestamps to strings for JSON serialization
        for key, value in company_info.items():
            if pd.isna(value):
                company_info[key] = None
            elif isinstance(value, (pd.Timestamp, datetime)):
                company_info[key] = value.strftime("%Y-%m-%d %H:%M:%S") if hasattr(value, 'strftime') else str(value)

        # Parse raw_data from JSON string if it's a string
        if company_info.get('raw_data') and isinstance(company_info['raw_data'], str):
            try:
                company_info['raw_data'] = json.loads(company_info['raw_data'])
            except json.JSONDecodeError:
                pass

        return company_info

    except Exception as e:
        print(f"Error loading company info: {str(e)}")
        return None


def get_founders(company_inn: str) -> List[Dict[str, Any]]:
    """
    Get founders list from company raw_data JSONB

    Args:
        company_inn: Company INN (Tax Identification Number)

    Returns:
        List of founders with name, percentage, is_individual, person_type
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        query = """
            SELECT
                jsonb_array_elements(raw_data->'founders') as founder
            FROM company_info
            WHERE inn = %s AND raw_data IS NOT NULL
        """

        cur.execute(query, (company_inn,))
        results = cur.fetchall()

        founders = []
        for row in results:
            if row[0]:
                founders.append(row[0])

        cur.close()
        conn.close()

        return founders

    except Exception as e:
        print(f"Error getting founders: {str(e)}")
        return []


# def get_business_metrics(user_id: int) -> Dict[str, int]:
#     """
#     Get business metrics from company raw_data JSONB
#     Now uses the new API structure with deals, licenses, buildings, cadastres

#     Args:
#         user_id: User ID

#     Returns:
#         Dictionary with deals, licenses, buildings, cadastres counts
#     """
#     try:
#         company_info = load_company_info(user_id)

#         if not company_info or not company_info.get('raw_data'):
#             return {
#                 'total_deals': 0,
#                 'customer_deals': 0,
#                 'provider_deals': 0,
#                 'total_licenses': 0,
#                 'total_buildings': 0,
#                 'total_cadastres': 0
#             }

#         raw_data = company_info['raw_data']

#         # Extract deals metrics from new API structure
#         deals_data = raw_data.get('deals', {})
#         deals_rows = deals_data.get('rows', [])
#         total_deals = deals_data.get('total', 0)

#         # Count customer vs provider deals from rows
#         customer_deals = sum(1 for d in deals_rows if d.get('type') == 1)
#         provider_deals = sum(1 for d in deals_rows if d.get('type') == 2)

#         # Extract licenses - now it's an array, not an object
#         licenses_data = raw_data.get('licenses', [])
#         if isinstance(licenses_data, list):
#             # Count only active licenses (deleted=0, status=1)
#             total_licenses = sum(1 for lic in licenses_data if lic.get('deleted') == 0 and lic.get('status') == 1)
#         else:
#             total_licenses = 0

#         # Buildings and cadastres - these come from the original entity API response
#         # They should be in the root of raw_data, not in a nested structure
#         buildings_data = raw_data.get('buildings', {})
#         cadastres_data = raw_data.get('cadastres', {})

#         total_buildings = buildings_data.get('total', 0) if isinstance(buildings_data, dict) else 0
#         total_cadastres = cadastres_data.get('total', 0) if isinstance(cadastres_data, dict) else 0

#         return {
#             'total_deals': total_deals,
#             'customer_deals': customer_deals,
#             'provider_deals': provider_deals,
#             'total_licenses': total_licenses,
#             'total_buildings': total_buildings,
#             'total_cadastres': total_cadastres
#         }

#     except Exception as e:
#         print(f"Error getting business metrics: {str(e)}")
#         return {
#             'total_deals': 0,
#             'customer_deals': 0,
#             'provider_deals': 0,
#             'total_licenses': 0,
#             'total_buildings': 0,
#             'total_cadastres': 0
#         }


def refresh_company_info(inn: str) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """
    Refresh company information from MyOrg API and update database
    Fetches data from all 6 MyOrg API endpoints

    Args:
        inn: Tax Identification Number

    Returns:
        (success, company_data, error_message) tuple
    """
    try:
        # 1. Fetch entity data (main company info)
        success, api_data, error = fetch_company_by_inn(inn)

        if not success:
            return False, None, error

        # Parse API response
        company_data = parse_myorg_response(api_data)

        # Get company_id for other API calls
        company_id = company_data.get('company_id')

        if not company_id:
            # If no company_id, still save entity data but skip extended data
            print(f"Warning: No company_id found for INN {inn}, skipping extended data fetch")
            save_success, save_error = save_company_info(company_data)
            if not save_success:
                return False, None, f"Failed to save company info: {save_error}"
            return True, company_data, None

        # 2. Fetch extended data from 5 additional APIs
        errors = []

        # Fetch deals
        deals_success, deals_data, deals_error = fetch_company_deals(company_id)
        if deals_success and deals_data:
            company_data['raw_data']['deals'] = deals_data
        elif deals_error:
            errors.append(f"Deals: {deals_error}")

        # Fetch court cases
        court_success, court_data, court_error = fetch_court_cases(inn)
        if court_success and court_data:
            company_data['raw_data']['court_cases'] = court_data
        elif court_error:
            errors.append(f"Court cases: {court_error}")

        # Fetch connections
        conn_success, conn_data, conn_error = fetch_company_connections(company_id)
        if conn_success and conn_data:
            company_data['raw_data']['connections'] = conn_data
        elif conn_error:
            errors.append(f"Connections: {conn_error}")

        # Fetch liabilities
        liab_success, liab_data, liab_error = fetch_company_liabilities(company_id)
        if liab_success and liab_data:
            company_data['raw_data']['liabilities'] = liab_data
        elif liab_error:
            errors.append(f"Liabilities: {liab_error}")

        # Fetch licenses
        lic_success, lic_data, lic_error = fetch_company_licenses(company_id)
        if lic_success and lic_data is not None:
            company_data['raw_data']['licenses'] = lic_data
        elif lic_error:
            errors.append(f"Licenses: {lic_error}")

        # 3. Save to database
        save_success, save_error = save_company_info(company_data)

        if not save_success:
            return False, None, f"Failed to save company info: {save_error}"

        # Return success with optional warnings about partial failures
        if errors:
            warning_msg = f"Some data could not be fetched: {'; '.join(errors)}"
            print(f"Warning: {warning_msg}")

        # Return the parsed data
        return True, company_data, None

    except Exception as e:
        return False, None, f"Error refreshing company info: {str(e)}"


def get_or_fetch_company_info(inn: str) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """
    Get company info from database, or fetch from API if not exists

    Args:
        inn: Tax Identification Number

    Returns:
        (success, company_data, error_message) tuple
    """
    # Try to load from database first
    company_info = load_company_info(inn)

    if company_info:
        return True, company_info, None

    # If not in database, fetch from API
    return refresh_company_info(inn)


def delete_company_info(inn: str) -> Tuple[bool, Optional[str]]:
    """
    Delete company information by INN

    Args:
        inn: Tax Identification Number

    Returns:
        (success, error_message) tuple
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("DELETE FROM company_info WHERE inn = %s", (inn,))
        conn.commit()

        return True, None

    except Exception as e:
        if conn:
            conn.rollback()
        return False, f"Error deleting company info: {str(e)}"

    finally:
        if conn:
            conn.close()


def get_company_inn_for_user(user_id: int) -> Optional[str]:
    """
    Get company INN for a user (lightweight query)

    Args:
        user_id: User ID

    Returns:
        INN string or None if not found
    """
    try:
        engine = get_db_engine()

        # Read user's configured company INN from users table (lightweight)
        query = "SELECT company_inn FROM users WHERE id = %(user_id)s"
        df = pd.read_sql_query(query, engine, params={'user_id': user_id})

        if df.empty:
            return None

        inn_value = df.iloc[0].get('company_inn')
        if pd.isna(inn_value):
            return None
        return str(inn_value) if inn_value is not None else None

    except Exception as e:
        print(f"Error getting company INN: {str(e)}")
        return None


def get_deals_summary(company_inn: str) -> Dict[str, Any]:
    """
    Get deals summary from company_deals table, fallback to raw_data JSONB

    Args:
        company_inn: Company INN (Tax Identification Number)

    Returns:
        Dictionary with deals metrics and list of recent deals
    """
    try:
        # Clean INN - remove .0 if present and strip whitespace
        company_inn = str(company_inn).replace('.0', '').strip()

        conn = get_db_connection()
        cur = conn.cursor()

        # Try company_deals table first
        try:
            query = """
                SELECT
                    deal_id,
                    deal_type,
                    deal_year,
                    deal_quarter,
                    counterparty_inn,
                    counterparty_name,
                    counterparty_company_id,
                    deal_amount,
                    deal_count,
                    raw_data
                FROM company_deals
                WHERE inn = %s
                ORDER BY deal_year DESC NULLS LAST, deal_quarter DESC NULLS LAST
            """

            cur.execute(query, (company_inn,))
            results = cur.fetchall()
            print(f"[DEBUG] Queried company_deals for INN {company_inn}, found {len(results)} results")

            if results:
                # Convert to list of dictionaries
                deals = []
                total_amount = 0
                customer_deals = 0
                provider_deals = 0

                for row in results:
                    deal_type = row[1]

                    if deal_type == 'customer':
                        customer_deals += 1
                    elif deal_type == 'provider':
                        provider_deals += 1

                    deal_amount = float(row[7]) if row[7] else 0
                    total_amount += deal_amount

                    raw_data = row[9] if row[9] else {}
                    deal_dict = {
                        'id': raw_data.get('id'),
                        'deal_id': row[0],
                        'type': 1 if deal_type == 'customer' else 2,
                        'deal_type': deal_type,
                        'deal_year': row[2],
                        'deal_quarter': row[3],
                        'deal_date': f"{row[2]}-{(row[3]-1)*3+1:02d}-01" if row[2] and row[3] else None,
                        'counterparty_inn': row[4],
                        'counterparty_name': row[5],
                        'customer_inn': row[4] if deal_type == 'customer' else company_inn,
                        'customer_name': row[5] if deal_type == 'customer' else None,
                        'provider_inn': row[4] if deal_type == 'provider' else company_inn,
                        'provider_name': row[5] if deal_type == 'provider' else None,
                        'deal_cost': str(deal_amount) if deal_amount else '0',
                        'deal_amount': deal_amount,
                        'amount': row[8],
                        'category_name': raw_data.get('category_name'),
                        'is_completed': raw_data.get('is_completed', 1),
                        'condition': raw_data.get('condition'),
                    }
                    deals.append(deal_dict)

                total_deals = len(results)
                avg_deal_size = total_amount / total_deals if total_deals > 0 else 0

                cur.close()
                conn.close()

                return {
                    'total_deals': total_deals,
                    'customer_deals': customer_deals,
                    'provider_deals': provider_deals,
                    'total_amount': total_amount,
                    'avg_deal_size': avg_deal_size,
                    'deals': deals
                }
        except Exception as table_error:
            print(f"Company_deals table not found or error: {table_error}, falling back to raw_data")

        cur.close()
        conn.close()

        # Fallback to raw_data JSONB
        company_info = load_company_info(company_inn)

        if not company_info or not company_info.get('raw_data'):
            return {
                'total_deals': 0,
                'customer_deals': 0,
                'provider_deals': 0,
                'total_amount': 0,
                'avg_deal_size': 0,
                'deals': []
            }

        raw_data = company_info['raw_data']
        deals_data = raw_data.get('deals', {})
        deals_rows = deals_data.get('rows', [])
        total_deals = deals_data.get('total', 0)

        if not deals_rows:
            return {
                'total_deals': 0,
                'customer_deals': 0,
                'provider_deals': 0,
                'total_amount': 0,
                'avg_deal_size': 0,
                'deals': []
            }

        # Count customer vs provider deals
        customer_deals = sum(1 for d in deals_rows if d.get('type') == 1)
        provider_deals = sum(1 for d in deals_rows if d.get('type') == 2)

        # Calculate total amount and average
        total_amount = sum(float(d.get('deal_cost', 0)) for d in deals_rows)
        avg_deal_size = total_amount / len(deals_rows) if deals_rows else 0

        return {
            'total_deals': total_deals,
            'customer_deals': customer_deals,
            'provider_deals': provider_deals,
            'total_amount': total_amount,
            'avg_deal_size': avg_deal_size,
            'deals': deals_rows
        }

    except Exception as e:
        print(f"Error getting deals summary: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'total_deals': 0,
            'customer_deals': 0,
            'provider_deals': 0,
            'total_amount': 0,
            'avg_deal_size': 0,
            'deals': []
        }


def get_court_cases_summary(company_inn: str) -> Dict[str, Any]:
    """
    Get court cases summary from company_court_cases table, fallback to raw_data JSONB

    Args:
        company_inn: Company INN (Tax Identification Number)

    Returns:
        Dictionary with court cases metrics and list
    """
    try:
        # Clean INN - remove .0 if present and strip whitespace
        company_inn = str(company_inn).replace('.0', '').strip()

        conn = get_db_connection()
        cur = conn.cursor()

        # Try company_court_cases table first
        try:
            query = """
                SELECT
                    case_number,
                    case_id,
                    case_type,
                    case_category,
                    court_name,
                    court_region,
                    plaintiff_name,
                    plaintiff_inn,
                    defendant_name,
                    defendant_inn,
                    case_status,
                    filing_date,
                    decision_date,
                    claim_amount,
                    decision_amount,
                    raw_data
                FROM company_court_cases
                WHERE inn = %s
                ORDER BY filing_date DESC NULLS LAST
            """

            cur.execute(query, (company_inn,))
            results = cur.fetchall()
            print(f"[DEBUG] Queried company_court_cases for INN {company_inn}, found {len(results)} results")

            if results:
                cases = []
                pending_cases = 0
                closed_cases = 0

                for row in results:
                    raw_data = row[15] if row[15] else {}
                    case_status_text = row[10]

                    is_closed = case_status_text and ('Якунланган' in case_status_text or 'Completed' in case_status_text or 'Closed' in case_status_text)

                    if is_closed:
                        closed_cases += 1
                        status = 0
                    else:
                        pending_cases += 1
                        status = 1

                    case_dict = {
                        'id': raw_data.get('id'),
                        'inn': company_inn,
                        'casenumber': row[0],
                        'case_number': row[0],
                        'case_id': row[1],
                        'type': row[2],
                        'case_type': row[2],
                        'category': row[3],
                        'case_category': row[3],
                        'court': row[4],
                        'court_name': row[4],
                        'region': row[5],
                        'court_region': row[5],
                        'claimant': row[6],
                        'plaintiff_name': row[6],
                        'plaintiff_inn': row[7],
                        'defendant': row[8],
                        'defendant_name': row[8],
                        'defendant_inn': row[9],
                        'status': status,
                        'status_name': case_status_text,
                        'case_status': case_status_text,
                        'reg_date': str(row[11]) if row[11] else None,
                        'filing_date': str(row[11]) if row[11] else None,
                        'hearing_date': str(row[12]) if row[12] else None,
                        'decision_date': str(row[12]) if row[12] else None,
                        'claim_amount': row[13],
                        'decision_amount': row[14],
                        'result': raw_data.get('result'),
                        'responsible': raw_data.get('responsible'),
                    }
                    cases.append(case_dict)

                total_cases = len(results)

                cur.close()
                conn.close()

                return {
                    'total_cases': total_cases,
                    'pending_cases': pending_cases,
                    'closed_cases': closed_cases,
                    'cases': cases
                }
        except Exception as table_error:
            print(f"Company_court_cases table not found or error: {table_error}, falling back to raw_data")

        cur.close()
        conn.close()

        # Fallback to raw_data JSONB
        company_info = load_company_info(company_inn)

        if not company_info or not company_info.get('raw_data'):
            return {
                'total_cases': 0,
                'pending_cases': 0,
                'closed_cases': 0,
                'cases': []
            }

        raw_data = company_info['raw_data']
        court_data = raw_data.get('court_cases', {})
        cases_rows = court_data.get('rows', [])
        total_cases = court_data.get('total', 0)

        if not cases_rows:
            return {
                'total_cases': 0,
                'pending_cases': 0,
                'closed_cases': 0,
                'cases': []
            }

        # Count pending vs closed
        pending_cases = sum(1 for c in cases_rows if c.get('status') != 0)
        closed_cases = sum(1 for c in cases_rows if c.get('status') == 0)

        return {
            'total_cases': total_cases,
            'pending_cases': pending_cases,
            'closed_cases': closed_cases,
            'cases': cases_rows
        }

    except Exception as e:
        print(f"Error getting court cases summary: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'total_cases': 0,
            'pending_cases': 0,
            'closed_cases': 0,
            'cases': []
        }


def get_connections_summary(company_inn: str) -> Dict[str, Any]:
    """
    Get connections summary from company_connections table, fallback to raw_data JSONB

    Args:
        company_inn: Company INN (Tax Identification Number)

    Returns:
        Dictionary with connections metrics and list
    """
    try:
        # Clean INN - remove .0 if present and strip whitespace
        company_inn = str(company_inn).replace('.0', '').strip()

        conn = get_db_connection()
        cur = conn.cursor()

        # Try company_connections table first
        try:
            query = """
                SELECT
                    connection_id,
                    connection_type,
                    connected_inn,
                    connected_name,
                    connected_company_id,
                    connected_uuid,
                    relationship,
                    ownership_percentage,
                    position,
                    is_active,
                    start_date,
                    end_date,
                    raw_data
                FROM company_connections
                WHERE inn = %s
                ORDER BY is_active DESC, start_date DESC NULLS LAST
            """

            cur.execute(query, (company_inn,))
            results = cur.fetchall()
            print(f"[DEBUG] Queried company_connections for INN {company_inn}, found {len(results)} results")

            if results:
                connections = []
                as_director = 0
                as_founder = 0

                for row in results:
                    raw_data = row[12] if row[12] else {}
                    connection_type = row[1]

                    is_director = raw_data.get('is_director', 0)

                    if is_director == 1:
                        as_director += 1
                    if connection_type == 2:
                        as_founder += 1

                    conn_dict = {
                        'id': raw_data.get('id') or row[4],
                        'connection_id': row[0],
                        'connection_type': connection_type,
                        'inn': row[2],
                        'connected_inn': row[2],
                        'name': row[3],
                        'connected_name': row[3],
                        'connected_company_id': row[4],
                        'uuid': row[5],
                        'director': row[8],
                        'position': row[8],
                        'relationship': row[6],
                        'ownership_percentage': row[7],
                        'is_director': is_director,
                        'is_active': row[9],
                        'activity_state': 1 if row[9] else 0,
                        'registration_date': str(row[10]) if row[10] else None,
                        'start_date': str(row[10]) if row[10] else None,
                        'address': raw_data.get('address'),
                        'region': raw_data.get('region'),
                        'oked_name': raw_data.get('oked_name'),
                        'oked_code': raw_data.get('oked_code'),
                    }
                    connections.append(conn_dict)

                total_connections = len(results)

                cur.close()
                conn.close()

                return {
                    'total_connections': total_connections,
                    'as_director': as_director,
                    'as_founder': as_founder,
                    'connections': connections
                }
        except Exception as table_error:
            print(f"Company_connections table not found or error: {table_error}, falling back to raw_data")

        cur.close()
        conn.close()

        # Fallback to raw_data JSONB
        company_info = load_company_info(company_inn)

        if not company_info or not company_info.get('raw_data'):
            return {
                'total_connections': 0,
                'as_director': 0,
                'as_founder': 0,
                'connections': []
            }

        raw_data = company_info['raw_data']
        connections_data = raw_data.get('connections', {})
        conn_rows = connections_data.get('rows', [])
        total_conn = connections_data.get('total', 0)

        if not conn_rows:
            return {
                'total_connections': 0,
                'as_director': 0,
                'as_founder': 0,
                'connections': []
            }

        # Count as_director and as_founder
        as_director = sum(1 for c in conn_rows if c.get('is_director') == 1)
        as_founder = sum(1 for c in conn_rows if c.get('connection_type') == 2)

        return {
            'total_connections': total_conn,
            'as_director': as_director,
            'as_founder': as_founder,
            'connections': conn_rows
        }

    except Exception as e:
        print(f"Error getting connections summary: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'total_connections': 0,
            'as_director': 0,
            'as_founder': 0,
            'connections': []
        }


def get_liabilities_summary(company_inn: str) -> Dict[str, Any]:
    """
    Get liabilities summary from company_liabilities table

    Args:
        company_inn: Company INN (Tax Identification Number)

    Returns:
        Dictionary with liabilities data
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Get all liabilities for the company
        query = """
            SELECT
                liability_id,
                liability_type,
                liability_category,
                creditor_name,
                creditor_inn,
                liability_amount,
                outstanding_amount,
                currency,
                liability_status,
                start_date,
                due_date,
                last_payment_date,
                collateral_description,
                interest_rate,
                raw_data
            FROM company_liabilities
            WHERE inn = %s
            ORDER BY due_date DESC NULLS LAST
        """

        cur.execute(query, (company_inn,))
        results = cur.fetchall()

        total = len(results)

        if total == 0:
            return {
                'has_liabilities': False,
                'total': 0,
                'data': None
            }

        # Build liabilities data structure from query results
        # For liabilities, we typically get aggregate data (total amount)
        total_liability_amount = 0
        
        for row in results:
            liability_amount = float(row[5]) if row[5] else 0  # liability_amount
            total_liability_amount += liability_amount

        cur.close()
        conn.close()

        return {
            'has_liabilities': total > 0,
            'total': total_liability_amount,  # Return total amount, not count
            'count': total,  # Number of liability records
            'data': {'total': total_liability_amount}
        }

    except Exception as e:
        print(f"Error getting liabilities summary: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'has_liabilities': False,
            'total': 0,
            'count': 0,
            'data': None
        }


def get_licenses_list(company_inn: str) -> List[Dict[str, Any]]:
    """
    Get licenses list from company_licenses table, fallback to raw_data JSONB

    Args:
        company_inn: Company INN (Tax Identification Number)

    Returns:
        List of active licenses
    """
    try:
        # Clean INN - remove .0 if present and strip whitespace
        company_inn = str(company_inn).replace('.0', '').strip()

        conn = get_db_connection()
        cur = conn.cursor()

        # Try company_licenses table first
        try:
            query = """
                SELECT
                    license_id,
                    license_number,
                    license_type,
                    license_category,
                    issuing_authority,
                    issuing_region,
                    license_name,
                    license_description,
                    activity_type,
                    license_status,
                    issue_date,
                    expiry_date,
                    last_renewal_date,
                    is_active,
                    scope_of_license,
                    raw_data
                FROM company_licenses
                WHERE inn = %s
                AND is_active = TRUE
                ORDER BY issue_date DESC NULLS LAST
            """

            cur.execute(query, (company_inn,))
            results = cur.fetchall()
            print(f"[DEBUG] Queried company_licenses for INN {company_inn}, found {len(results)} results")

            if results:
                active_licenses = []
                for row in results:
                    raw_data = row[15] if row[15] else {}

                    license_dict = {
                        'id': raw_data.get('id'),
                        'inn': company_inn,
                        'license_id': row[0],
                        'number': row[1],
                        'license_number': row[1],
                        'type': row[2],
                        'license_type': row[2],
                        'category': row[3],
                        'license_category': row[3],
                        'category_en': row[3],
                        'issuing_authority': row[4],
                        'issuing_region': row[5],
                        'title': row[6],
                        'license_name': row[6],
                        'name': row[7],
                        'license_description': row[7],
                        'activity_type': row[8],
                        'specializations': raw_data.get('specializations', []),
                        'status': 1 if row[13] else 0,
                        'license_status': row[9],
                        'date_of_issue': str(row[10]) if row[10] else None,
                        'issue_date': str(row[10]) if row[10] else None,
                        'validity': str(row[11]) if row[11] else None,
                        'expiry_date': str(row[11]) if row[11] else None,
                        'last_renewal_date': str(row[12]) if row[12] else None,
                        'is_active': row[13],
                        'register_number': row[14],
                        'deleted': 0,
                    }
                    active_licenses.append(license_dict)

                cur.close()
                conn.close()

                return active_licenses
        except Exception as table_error:
            print(f"Company_licenses table not found or error: {table_error}, falling back to raw_data")

        cur.close()
        conn.close()

        # Fallback to raw_data JSONB
        company_info = load_company_info(company_inn)

        if not company_info or not company_info.get('raw_data'):
            return []

        raw_data = company_info['raw_data']
        licenses_data = raw_data.get('licenses', [])

        if not isinstance(licenses_data, list):
            return []

        # Filter active licenses (deleted=0, status=1)
        active_licenses = [lic for lic in licenses_data if lic.get('deleted') == 0 and lic.get('status') == 1]

        return active_licenses

    except Exception as e:
        print(f"Error getting licenses list: {str(e)}")
        import traceback
        traceback.print_exc()
        return []


def get_ratings_summary(company_inn: str) -> Dict[str, Any]:
    """
    Get company ratings summary from company_ratings table

    Args:
        company_inn: Company INN (Tax Identification Number)

    Returns:
        Dictionary with current rating data and history
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Get current rating
        current_rating_query = """
            SELECT
                rating_id,
                rating_type,
                rating_agency,
                rating_score,
                rating_grade,
                rating_category,
                credit_limit,
                risk_level,
                risk_category,
                default_probability,
                liquidity_ratio,
                solvency_ratio,
                profitability_ratio,
                rating_date,
                valid_from,
                valid_until,
                rating_outlook,
                rating_notes,
                raw_data
            FROM company_ratings
            WHERE inn = %s
            AND is_current = TRUE
            ORDER BY rating_date DESC
            LIMIT 1
        """

        cur.execute(current_rating_query, (company_inn,))
        current_result = cur.fetchone()

        # Get rating history
        history_query = """
            SELECT
                rating_grade,
                rating_score,
                risk_level,
                rating_date,
                rating_outlook,
                raw_data
            FROM company_ratings
            WHERE inn = %s
            ORDER BY rating_date DESC
            LIMIT 10
        """

        cur.execute(history_query, (company_inn,))
        history_results = cur.fetchall()

        cur.close()
        conn.close()

        if not current_result:
            return {
                'has_rating': False,
                'current_rating': None,
                'history': []
            }

        # Build current rating from raw_data
        current_raw_data = current_result[18] if current_result[18] else {}

        current_rating = {
            'rating_grade': current_result[4],
            'rating_score': float(current_result[3]) if current_result[3] else None,
            'risk_level': current_result[7],
            'credit_limit': float(current_result[6]) if current_result[6] else None,
            'rating_outlook': current_result[16],
            'rating_date': current_result[13].strftime("%Y-%m-%d") if current_result[13] else None,
            'raw_data': current_raw_data
        }

        # Build history
        history = []
        for row in history_results:
            history.append({
                'rating_grade': row[0],
                'rating_score': float(row[1]) if row[1] else None,
                'risk_level': row[2],
                'rating_date': row[3].strftime("%Y-%m-%d") if row[3] else None,
                'rating_outlook': row[4]
            })

        return {
            'has_rating': True,
            'current_rating': current_rating,
            'history': history
        }

    except Exception as e:
        print(f"Error getting ratings summary: {str(e)}")
        return {
            'has_rating': False,
            'current_rating': None,
            'history': []
        }


def get_collaterals_list(company_inn: str) -> List[Dict[str, Any]]:
    """
    Get collaterals list from company_collaterals table

    Args:
        company_inn: Company INN (Tax Identification Number)

    Returns:
        List of active collaterals/pledges
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Get all active collaterals for the company
        query = """
            SELECT
                collateral_id,
                collateral_number,
                collateral_type,
                collateral_category,
                asset_description,
                asset_type,
                asset_location,
                collateral_value,
                assessed_value,
                currency,
                pledgee_name,
                pledgee_inn,
                pledgor_name,
                pledgor_inn,
                collateral_status,
                registration_date,
                expiry_date,
                release_date,
                is_active,
                raw_data
            FROM company_collaterals
            WHERE inn = %s
            AND is_active = TRUE
            ORDER BY registration_date DESC NULLS LAST
        """

        cur.execute(query, (company_inn,))
        results = cur.fetchall()

        if not results:
            return []

        # Convert to list of dictionaries from raw_data
        collaterals = []
        for row in results:
            raw_data = row[19] if row[19] else {}
            collaterals.append(raw_data)

        cur.close()
        conn.close()

        return collaterals

    except Exception as e:
        print(f"Error getting collaterals list: {str(e)}")
        return []


def set_company_inn_for_user(user_id: int, inn: str) -> Tuple[bool, Optional[str]]:
    """
    Set or update the user's company INN in users table

    Args:
        user_id: User ID
        inn: Tax Identification Number (9 digits)

    Returns:
        (success, error_message)
    """
    conn = None
    try:
        if not inn or not str(inn).isdigit() or len(str(inn)) != 9:
            return False, "Invalid INN format. Must be exactly 9 digits"

        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("UPDATE users SET company_inn = %s WHERE id = %s", (str(inn), user_id))
        conn.commit()
        return True, None
    except Exception as e:
        if conn:
            conn.rollback()
        return False, f"Error setting company INN: {str(e)}"
    finally:
        if conn:
            conn.close()


# ============================================================================
# VIEWED COMPANIES CACHE - For "View Details" Feature
# ============================================================================


def load_viewed_company(inn: str) -> Optional[Dict[str, Any]]:
    """
    Load a viewed company from cache

    Args:
        inn: Tax Identification Number

    Returns:
        Dictionary with company information or None if not found
    """
    try:
        engine = get_db_engine()

        query = """
            SELECT
                id, inn, company_name, uuid, company_id, status, status_description, activity_state,
                registration_number, registration_date, registration_center,
                statutory_fund, director_name, is_small_business,
                enterprise_category, taxation_type,
                oked_code, oked_description,
                opf_code, opf_description,
                soogu_code, soogu_description,
                soato_code, soato_description,
                region, city, street_address, email, phone,
                village_code, village_name,
                trust, score, itpark, is_bankrupt, is_abuse_vat, is_large_taxpayer,
                relevance_date, is_verified,
                raw_data, last_fetched, fetch_count, created_at
            FROM viewed_companies
            WHERE inn = %(inn)s
        """

        df = pd.read_sql_query(query, engine, params={'inn': str(inn).strip()})

        if df.empty:
            return None

        # Convert first row to dictionary
        company_info = df.iloc[0].to_dict()

        # Convert pandas timestamps to strings
        for key, value in company_info.items():
            if pd.isna(value):
                company_info[key] = None
            elif isinstance(value, (pd.Timestamp, datetime)):
                company_info[key] = value.strftime("%Y-%m-%d %H:%M:%S") if hasattr(value, 'strftime') else str(value)

        # Parse raw_data from JSON string if needed
        if company_info.get('raw_data') and isinstance(company_info['raw_data'], str):
            try:
                company_info['raw_data'] = json.loads(company_info['raw_data'])
            except json.JSONDecodeError:
                pass

        return company_info

    except Exception as e:
        print(f"Error loading viewed company: {str(e)}")
        return None


def save_viewed_company(company_data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Save or update a viewed company in cache

    Args:
        company_data: Dictionary with company information (includes 'raw_data' key)

    Returns:
        (success, error_message) tuple
    """
    if not company_data or not company_data.get('inn'):
        return False, "Invalid company data - INN is required"

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Check if company already exists in cache
        cur.execute("SELECT id FROM viewed_companies WHERE inn = %s", (company_data.get('inn'),))
        existing = cur.fetchone()

        # Extract raw_data for JSONB storage
        raw_data_json = json.dumps(company_data.get('raw_data', {}))

        if existing:
            # Update existing cached record
            update_query = """
                UPDATE viewed_companies SET
                    company_name = %s,
                    uuid = %s,
                    company_id = %s,
                    status = %s,
                    status_description = %s,
                    activity_state = %s,
                    registration_number = %s,
                    registration_date = %s,
                    registration_center = %s,
                    statutory_fund = %s,
                    director_name = %s,
                    is_small_business = %s,
                    enterprise_category = %s,
                    taxation_type = %s,
                    oked_code = %s,
                    oked_description = %s,
                    opf_code = %s,
                    opf_description = %s,
                    soogu_code = %s,
                    soogu_description = %s,
                    soato_code = %s,
                    soato_description = %s,
                    region = %s,
                    city = %s,
                    street_address = %s,
                    email = %s,
                    phone = %s,
                    village_code = %s,
                    village_name = %s,
                    trust = %s,
                    score = %s,
                    itpark = %s,
                    is_bankrupt = %s,
                    is_abuse_vat = %s,
                    is_large_taxpayer = %s,
                    relevance_date = %s,
                    is_verified = %s,
                    raw_data = %s::jsonb
                WHERE inn = %s
            """
            cur.execute(update_query, (
                company_data.get('company_name'),
                company_data.get('uuid'),
                company_data.get('company_id'),
                company_data.get('status'),
                company_data.get('status_description'),
                company_data.get('activity_state'),
                company_data.get('registration_number'),
                company_data.get('registration_date'),
                company_data.get('registration_center'),
                company_data.get('statutory_fund'),
                company_data.get('director_name'),
                company_data.get('is_small_business', False),
                company_data.get('enterprise_category'),
                company_data.get('taxation_type'),
                company_data.get('oked_code'),
                company_data.get('oked_description'),
                company_data.get('opf_code'),
                company_data.get('opf_description'),
                company_data.get('soogu_code'),
                company_data.get('soogu_description'),
                company_data.get('soato_code'),
                company_data.get('soato_description'),
                company_data.get('region'),
                company_data.get('city'),
                company_data.get('street_address'),
                company_data.get('email'),
                company_data.get('phone'),
                company_data.get('village_code'),
                company_data.get('village_name'),
                company_data.get('trust'),
                company_data.get('score'),
                company_data.get('itpark', False),
                company_data.get('is_bankrupt', False),
                company_data.get('is_abuse_vat', False),
                company_data.get('is_large_taxpayer', False),
                company_data.get('relevance_date'),
                company_data.get('is_verified', False),
                raw_data_json,
                company_data.get('inn')
            ))
        else:
            # Insert new cached record
            insert_query = """
                INSERT INTO viewed_companies (
                    inn, company_name, uuid, company_id, status, status_description, activity_state,
                    registration_number, registration_date, registration_center,
                    statutory_fund, director_name, is_small_business,
                    enterprise_category, taxation_type,
                    oked_code, oked_description,
                    opf_code, opf_description,
                    soogu_code, soogu_description,
                    soato_code, soato_description,
                    region, city, street_address, email, phone,
                    village_code, village_name,
                    trust, score, itpark, is_bankrupt, is_abuse_vat, is_large_taxpayer,
                    relevance_date, is_verified, raw_data
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
                )
            """
            cur.execute(insert_query, (
                company_data.get('inn'),
                company_data.get('company_name'),
                company_data.get('uuid'),
                company_data.get('company_id'),
                company_data.get('status'),
                company_data.get('status_description'),
                company_data.get('activity_state'),
                company_data.get('registration_number'),
                company_data.get('registration_date'),
                company_data.get('registration_center'),
                company_data.get('statutory_fund'),
                company_data.get('director_name'),
                company_data.get('is_small_business', False),
                company_data.get('enterprise_category'),
                company_data.get('taxation_type'),
                company_data.get('oked_code'),
                company_data.get('oked_description'),
                company_data.get('opf_code'),
                company_data.get('opf_description'),
                company_data.get('soogu_code'),
                company_data.get('soogu_description'),
                company_data.get('soato_code'),
                company_data.get('soato_description'),
                company_data.get('region'),
                company_data.get('city'),
                company_data.get('street_address'),
                company_data.get('email'),
                company_data.get('phone'),
                company_data.get('village_code'),
                company_data.get('village_name'),
                company_data.get('trust'),
                company_data.get('score'),
                company_data.get('itpark', False),
                company_data.get('is_bankrupt', False),
                company_data.get('is_abuse_vat', False),
                company_data.get('is_large_taxpayer', False),
                company_data.get('relevance_date'),
                company_data.get('is_verified', False),
                raw_data_json
            ))

        conn.commit()
        return True, None

    except Exception as e:
        if conn:
            conn.rollback()
        error_msg = f"Error saving viewed company: {str(e)}"
        print(error_msg)
        return False, error_msg

    finally:
        if conn:
            conn.close()


def fetch_and_cache_viewed_company(inn: str) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """
    Fetch company information from all 6 MyOrg APIs and cache it

    Args:
        inn: Tax Identification Number

    Returns:
        (success, company_data, error_message) tuple
    """
    try:
        # 1. Fetch entity data (main company info)
        success, api_data, error = fetch_company_by_inn(inn)

        if not success:
            return False, None, error

        # Parse API response
        company_data = parse_myorg_response(api_data)

        # Get company_id for other API calls
        company_id = company_data.get('company_id')

        if not company_id:
            # If no company_id, still save entity data but skip extended data
            print(f"Warning: No company_id found for INN {inn}, skipping extended data fetch")
            save_success, save_error = save_viewed_company(company_data)
            if not save_success:
                return False, None, f"Failed to save company: {save_error}"
            return True, company_data, None

        # 2. Fetch extended data from 5 additional APIs
        errors = []

        # Fetch deals
        deals_success, deals_data, deals_error = fetch_company_deals(company_id)
        if deals_success and deals_data:
            company_data['raw_data']['deals'] = deals_data
        elif deals_error:
            errors.append(f"Deals: {deals_error}")

        # Fetch court cases
        court_success, court_data, court_error = fetch_court_cases(inn)
        if court_success and court_data:
            company_data['raw_data']['court_cases'] = court_data
        elif court_error:
            errors.append(f"Court cases: {court_error}")

        # Fetch connections
        conn_success, conn_data, conn_error = fetch_company_connections(company_id)
        if conn_success and conn_data:
            company_data['raw_data']['connections'] = conn_data
        elif conn_error:
            errors.append(f"Connections: {conn_error}")

        # Fetch liabilities
        liab_success, liab_data, liab_error = fetch_company_liabilities(company_id)
        if liab_success and liab_data:
            company_data['raw_data']['liabilities'] = liab_data
        elif liab_error:
            errors.append(f"Liabilities: {liab_error}")

        # Fetch licenses
        lic_success, lic_data, lic_error = fetch_company_licenses(company_id)
        if lic_success and lic_data is not None:
            company_data['raw_data']['licenses'] = lic_data
        elif lic_error:
            errors.append(f"Licenses: {lic_error}")

        # 3. Save to cache
        save_success, save_error = save_viewed_company(company_data)

        if not save_success:
            return False, None, f"Failed to cache company: {save_error}"

        # Return with optional warnings
        if errors:
            warning_msg = f"Some data could not be fetched: {'; '.join(errors)}"
            print(f"Warning: {warning_msg}")

        return True, company_data, None

    except Exception as e:
        return False, None, f"Error fetching company: {str(e)}"


def get_or_fetch_viewed_company(inn: str, cache_ttl_hours: int = 24) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """
    Get company from cache if recent, otherwise fetch from API

    Args:
        inn: Tax Identification Number
        cache_ttl_hours: Cache time-to-live in hours (default 24)

    Returns:
        (success, company_data, error_message) tuple
    """
    try:
        # Clean INN
        inn = str(inn).replace(' ', '').strip()

        # Try to load from cache first
        cached_company = load_viewed_company(inn)

        if cached_company:
            # Check if cache is still fresh
            last_fetched = cached_company.get('last_fetched')
            if last_fetched:
                if isinstance(last_fetched, str):
                    last_fetched = datetime.strptime(last_fetched, "%Y-%m-%d %H:%M:%S")

                time_since_fetch = datetime.now() - last_fetched
                hours_since_fetch = time_since_fetch.total_seconds() / 3600

                if hours_since_fetch < cache_ttl_hours:
                    # Cache is fresh, return it
                    print(f"Returning cached data for INN {inn} (age: {hours_since_fetch:.1f} hours)")
                    return True, cached_company, None
                else:
                    print(f"Cache stale for INN {inn} (age: {hours_since_fetch:.1f} hours), fetching fresh data")

        # Cache doesn't exist or is stale, fetch from API
        return fetch_and_cache_viewed_company(inn)

    except Exception as e:
        return False, None, f"Error getting viewed company: {str(e)}"


def cleanup_old_viewed_companies(days_old: int = 30) -> Tuple[int, Optional[str]]:
    """
    Delete cached viewed companies older than specified days

    Args:
        days_old: Delete entries older than this many days

    Returns:
        (deleted_count, error_message) tuple
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Delete old entries
        cur.execute("""
            DELETE FROM viewed_companies
            WHERE last_fetched < CURRENT_TIMESTAMP - INTERVAL '%s days'
        """, (days_old,))

        deleted_count = cur.rowcount
        conn.commit()

        print(f"Cleaned up {deleted_count} old viewed companies (older than {days_old} days)")
        return deleted_count, None

    except Exception as e:
        if conn:
            conn.rollback()
        error_msg = f"Error cleaning up viewed companies: {str(e)}"
        print(error_msg)
        return 0, error_msg

    finally:
        if conn:
            conn.close()


def get_my_counterparties(user_id: str) -> pd.DataFrame:
    """
    Get all counterparties from invoices and bank transactions

    This function aggregates unique counterparty INNs from three sources:
    1. Invoices OUT (buyer_inn) - Customers
    2. Invoices IN (seller_inn) - Suppliers
    3. Bank transactions (counterparty_inn) - Payment counterparties

    Args:
        user_id: User identifier

    Returns:
        DataFrame with columns:
        - inn: Tax ID
        - name: Counterparty name from transactions
        - source: Comma-separated list of sources (invoice_out, invoice_in, bank_statement)
        - transaction_count: Total number of transactions across all sources
        - invoice_amount: Total amount from invoices
        - bank_amount: Total amount from bank statements
        - total_amount: Total transaction volume (invoice_amount + bank_amount)
        - last_invoice_date: Most recent invoice date
        - last_transaction_date: Most recent bank transaction date
    """
    try:
        engine = get_db_engine()
        user_id = str(user_id)

        # Query 1: Get buyers from OUT invoices (customers)
        query_out = """
            SELECT
                buyer_inn as inn,
                MAX(buyer_name) as name,
                'invoice_out' as source,
                COUNT(*) as transaction_count,
                SUM(total_amount) as invoice_amount,
                MAX(document_date) as last_invoice_date,
                NULL::DATE as last_transaction_date,
                0::DECIMAL as bank_amount
            FROM invoices
            WHERE user_id = %(user_id)s
              AND invoice_type = 'OUT'
              AND buyer_inn IS NOT NULL
              AND buyer_inn != ''
              AND buyer_inn != 'nan'
            GROUP BY buyer_inn
        """

        # Query 2: Get sellers from IN invoices (suppliers)
        query_in = """
            SELECT
                seller_inn as inn,
                MAX(seller_name) as name,
                'invoice_in' as source,
                COUNT(*) as transaction_count,
                SUM(total_amount) as invoice_amount,
                MAX(document_date) as last_invoice_date,
                NULL::DATE as last_transaction_date,
                0::DECIMAL as bank_amount
            FROM invoices
            WHERE user_id = %(user_id)s
              AND invoice_type = 'IN'
              AND seller_inn IS NOT NULL
              AND seller_inn != ''
              AND seller_inn != 'nan'
            GROUP BY seller_inn
        """

        # Query 3: Get counterparties from bank statements
        query_bank = """
            SELECT
                counterparty_inn as inn,
                MAX(counterparty_name) as name,
                'bank_statement' as source,
                COUNT(*) as transaction_count,
                0::DECIMAL as invoice_amount,
                NULL::DATE as last_invoice_date,
                MAX(transaction_date) as last_transaction_date,
                SUM(amount) as bank_amount
            FROM bank_transactions
            WHERE user_id = %(user_id)s
              AND counterparty_inn IS NOT NULL
              AND counterparty_inn != ''
              AND counterparty_inn != 'nan'
            GROUP BY counterparty_inn
        """

        params = {'user_id': user_id}

        # Execute queries
        df_out = pd.read_sql_query(query_out, engine, params=params)
        df_in = pd.read_sql_query(query_in, engine, params=params)
        df_bank = pd.read_sql_query(query_bank, engine, params=params)

        # Combine all sources
        all_counterparties = pd.concat([df_out, df_in, df_bank], ignore_index=True)

        if all_counterparties.empty:
            print("No counterparties found")
            return pd.DataFrame()

        # Clean INNs
        all_counterparties['inn'] = all_counterparties['inn'].astype(str).str.replace('.0', '', regex=False).str.strip()

        # Convert date columns to datetime, handling NULL values
        if 'last_invoice_date' in all_counterparties.columns:
            all_counterparties['last_invoice_date'] = pd.to_datetime(all_counterparties['last_invoice_date'], errors='coerce')
        if 'last_transaction_date' in all_counterparties.columns:
            all_counterparties['last_transaction_date'] = pd.to_datetime(all_counterparties['last_transaction_date'], errors='coerce')

        # Helper function for date max that handles all-NaN groups
        def safe_date_max(series):
            """Get max date, returning NaT if all values are NaN"""
            valid_dates = series.dropna()
            return valid_dates.max() if len(valid_dates) > 0 else pd.NaT

        # Helper function for source aggregation
        def combine_sources(series):
            """Combine sources into comma-separated string"""
            sources = series.dropna().astype(str)
            return ', '.join(sorted(set(sources))) if len(sources) > 0 else ''

        # Group by INN and aggregate across sources
        aggregated = all_counterparties.groupby('inn').agg({
            'name': 'first',  # Take first non-null name
            'source': combine_sources,
            'transaction_count': 'sum',
            'invoice_amount': 'sum',
            'bank_amount': 'sum',
            'last_invoice_date': safe_date_max,
            'last_transaction_date': safe_date_max
        }).reset_index()

        # Calculate total amount (invoice + bank)
        aggregated['total_amount'] = aggregated['invoice_amount'] + aggregated['bank_amount']

        # Sort by total amount descending
        result = aggregated.sort_values('total_amount', ascending=False)
        
        if len(result) > 0:
            print("Result:", result.iloc[0])

        return result

    except Exception as e:
        print(f"Error getting counterparties: {str(e)}")
        import traceback
        traceback.print_exc()

        return pd.DataFrame()
