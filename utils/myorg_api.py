"""
MyOrg API Integration
Fetches company information from MyOrg API using Bearer token authentication
Based on real MyOrg API: https://api.myorg.uz/api/entity/inn/{inn}
"""
import requests
import json
import time
from typing import Optional, Dict, Any, Tuple, List, Callable
from datetime import datetime
import streamlit as st


def get_myorg_config() -> Tuple[Optional[str], Optional[str]]:
    """
    Get MyOrg API configuration from Streamlit secrets

    Returns:
        (base_url, bearer_token) tuple
    """
    try:
        base_url = st.secrets.get("myorg_api", {}).get("base_url")
        bearer_token = st.secrets.get("myorg_api", {}).get("bearer_token")
        return base_url, bearer_token
    except Exception as e:
        print(f"Error reading MyOrg API config: {str(e)}")
        return None, None


def retry_request(
    request_func: Callable[[], requests.Response],
    endpoint_name: str,
    max_retries: int = 10,
    retry_delay: float = 2.0
) -> requests.Response:
    """
    Generic retry wrapper for API requests.
    Retries on timeout and 504 Gateway Timeout errors.

    Args:
        request_func: Function that makes the HTTP request
        endpoint_name: Name of the endpoint for logging
        max_retries: Maximum number of retry attempts (default: 10)
        retry_delay: Delay between retries in seconds (default: 2.0)

    Returns:
        Response object

    Raises:
        Exception: If all retries are exhausted
    """
    for attempt in range(1, max_retries + 1):
        try:
            if attempt > 1:
                print(f"    → Retry attempt {attempt}/{max_retries} for {endpoint_name} after {retry_delay}s delay...")
                time.sleep(retry_delay)

            response = request_func()

            # If we get 504, retry
            if response.status_code == 504:
                print(f"    ⚠ Received 504 Gateway Timeout for {endpoint_name} (attempt {attempt}/{max_retries})")
                if attempt < max_retries:
                    continue
                else:
                    raise Exception(f"API request failed after {max_retries} attempts: 504 Gateway Timeout")

            # For any other status code (including success), return immediately
            return response

        except requests.exceptions.Timeout:
            print(f"    ⚠ Request timed out for {endpoint_name} (attempt {attempt}/{max_retries})")
            if attempt < max_retries:
                continue
            else:
                raise Exception(f"API request timed out after {max_retries} attempts")

        except requests.exceptions.ConnectionError as e:
            # Connection errors - retry
            print(f"    ⚠ Connection error for {endpoint_name} (attempt {attempt}/{max_retries}): {str(e)}")
            if attempt < max_retries:
                continue
            else:
                raise Exception(f"Failed to connect after {max_retries} attempts: {str(e)}")

        except requests.exceptions.RequestException as e:
            # Other request exceptions - don't retry, fail immediately
            raise e

    # Should never reach here
    raise Exception(f"API request failed after {max_retries} attempts")


def fetch_company_by_inn(inn: str, base_url: Optional[str] = None, bearer_token: Optional[str] = None) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """
    Fetch company information from MyOrg API by INN

    Real API endpoint: GET /api/entity/inn/{inn}
    Auth header: token: Bearer {jwt_token}

    Args:
        inn: Tax Identification Number (9 digits)
        base_url: Optional override for API base URL (uses secrets if not provided)
        bearer_token: Optional override for bearer token (uses secrets if not provided)

    Returns:
        (success, data, error_message) tuple
        - success: Boolean indicating if request was successful
        - data: Complete API response dictionary or None if failed
        - error_message: Error message if failed, None if successful
    """
    # Get config from secrets if not provided
    if not base_url or not bearer_token:
        config_url, config_token = get_myorg_config()
        base_url = base_url or config_url
        bearer_token = bearer_token or config_token

    if not base_url or not bearer_token:
        return False, None, "MyOrg API configuration not found in secrets"

    if not inn:
        return False, None, "INN (Tax ID) is required"

    # Clean INN (remove any whitespace)
    inn = str(inn).replace(' ', '').strip()

    # Validate INN format (9 digits)
    if not inn.isdigit() or len(inn) != 9:
        return False, None, f"Invalid INN format: {inn}. INN must be exactly 9 digits"

    try:
        # Construct API endpoint: https://api.myorg.uz/api/entity/inn/312387381
        url = f"{base_url.rstrip('/')}/api/entity/inn/{inn}"

        # IMPORTANT: MyOrg uses 'token' header, not 'Authorization'
        headers = {
            "token": f"Bearer {bearer_token}",
            "accept": "application/json"
        }

        print(f"Fetching company data from MyOrg for INN: {inn}")

        # Make API request with timeout
        response = requests.get(url, headers=headers, timeout=30)

        # Check if request was successful
        if response.status_code == 200:
            data = response.json()
            print(f"Successfully fetched company data for INN: {inn}")
            return True, data, None
        elif response.status_code == 404:
            return False, None, f"Company with INN {inn} not found in MyOrg"
        elif response.status_code == 401:
            return False, None, "Authentication failed - invalid bearer token"
        elif response.status_code == 403:
            return False, None, "Access forbidden - insufficient permissions"
        else:
            return False, None, f"API request failed with status {response.status_code}: {response.text}"

    except requests.exceptions.Timeout:
        return False, None, "API request timed out (30s)"
    except requests.exceptions.ConnectionError:
        return False, None, "Failed to connect to MyOrg API - check network connection"
    except requests.exceptions.RequestException as e:
        return False, None, f"API request error: {str(e)}"
    except Exception as e:
        return False, None, f"Unexpected error: {str(e)}"


def parse_myorg_response(api_response: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse MyOrg API response and extract company information
    Maps real MyOrg API fields to database schema

    Args:
        api_response: Raw API response dictionary from /api/entity/inn/{inn}

    Returns:
        Dictionary with standardized company information + raw_data
    """
    # Helper functions
    def get_value(data: Dict, key: str, default=None):
        """Safely get value from dictionary"""
        return data.get(key, default) if data else default

    def parse_date(date_str: str) -> Optional[str]:
        """Parse date string to YYYY-MM-DD format"""
        if not date_str:
            return None
        try:
            # MyOrg returns dates in YYYY-MM-DD format already
            if isinstance(date_str, str):
                # Try to parse to validate
                dt = datetime.strptime(date_str, "%Y-%m-%d")
                return dt.strftime("%Y-%m-%d")
            return None
        except Exception:
            return None

    def parse_int(value: Any) -> Optional[int]:
        """Parse integer values"""
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def clean_decimal(value: Any) -> Optional[int]:
        """Clean and parse decimal values (convert to int)"""
        if value is None:
            return None
        try:
            # Remove spaces and convert to int
            cleaned = str(value).replace(' ', '').replace(',', '')
            return int(float(cleaned))
        except (ValueError, AttributeError):
            return None

    def to_text(value: Any) -> Optional[str]:
        """Convert dict/list to JSON text; leave strings/numbers as-is."""
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            try:
                return json.dumps(value, ensure_ascii=False)
            except Exception:
                return str(value)
        return value

    def extract_name(detail_obj: Any, lang: str = 'ru') -> Optional[str]:
        """
        Extract name from detail objects that have multiple language fields.
        Example: {"id": 2, "name": "Малый бизнес", "name_uz": "Kichik biznes", "name_en": "Small business"}
        Returns just the name in the preferred language.
        """
        if detail_obj is None:
            return None

        if isinstance(detail_obj, str):
            return detail_obj

        if not isinstance(detail_obj, dict):
            return str(detail_obj)

        # Try language-specific name first
        lang_key = f'name_{lang}'
        if lang_key in detail_obj:
            return detail_obj[lang_key]

        # Fallback to 'name' field (usually Russian)
        if 'name' in detail_obj:
            return detail_obj['name']

        # Last resort: return first name-like field
        for key in ['name_ru', 'name_en', 'name_uz']:
            if key in detail_obj:
                return detail_obj[key]

        # If no name field found, return the whole object as JSON
        return json.dumps(detail_obj, ensure_ascii=False)

    # Get nested activity_state_detail
    activity_state_detail = get_value(api_response, 'activity_state_detail', {})

    # Get village_detail
    village_detail = get_value(api_response, 'village_detail', {})

    # Get phones (array)
    phones = get_value(api_response, 'phones', [])
    phone = phones[0] if phones and len(phones) > 0 else None

    # Coerce common numeric fields once to avoid type issues from API
    trust_value = parse_int(get_value(api_response, 'trust'))
    score_value = parse_int(get_value(api_response, 'score'))
    itpark_value = parse_int(get_value(api_response, 'itpark'))
    is_bankrupt_value = parse_int(get_value(api_response, 'is_bankrupt'))
    is_abuse_vat_value = parse_int(get_value(api_response, 'is_abuse_vat'))
    is_large_taxpayer_value = parse_int(get_value(api_response, 'is_large_taxpayer'))
    company_id_value = parse_int(get_value(api_response, 'id'))

    # Map API response to database schema
    company_data = {
        # Store complete raw response
        'raw_data': api_response,

        # Basic information (from API response)
        'inn': to_text(get_value(api_response, 'inn')),
        'company_name': to_text(get_value(api_response, 'name')),
        'uuid': get_value(api_response, 'uuid'),
        'company_id': company_id_value,

        # Status (from activity_state_detail)
        'activity_state': parse_int(get_value(api_response, 'activity_state')),
        'status': to_text(get_value(activity_state_detail, 'group')),
        'status_description': to_text(get_value(activity_state_detail, 'name')),

        # Registration details
        'registration_number': to_text(get_value(api_response, 'registration_number')),
        'registration_date': parse_date(get_value(api_response, 'registration_date')),
        'registration_center': to_text(get_value(api_response, 'registration_authority')),

        # Financial and legal info
        'statutory_fund': clean_decimal(get_value(api_response, 'statutory_fund')),
        'director_name': to_text(get_value(api_response, 'director')),
        'is_small_business': get_value(api_response, 'small_businesses') is not None,
        'enterprise_category': extract_name(get_value(api_response, 'business_type_detail')),
        'taxation_type': extract_name(get_value(api_response, 'tax_mode_detail')) if get_value(api_response, 'tax_mode_detail') else (f"Tax mode: {get_value(api_response, 'tax_mode')}" if get_value(api_response, 'tax_mode') is not None else None),

        # Statistical codes - OKED (Activity)
        'oked_code': to_text(get_value(api_response, 'oked_code')),
        'oked_description': to_text(get_value(api_response, 'oked_name')),

        # Statistical codes - OPF (Organizational form)
        'opf_code': to_text(get_value(api_response, 'opf_code')),
        'opf_description': to_text(get_value(api_response, 'opf_name')),

        # Statistical codes - SOOGU
        'soogu_code': to_text(get_value(api_response, 'soogu_code')),
        'soogu_description': to_text(get_value(api_response, 'soogu_name')),

        # Statistical codes - SOATO (Territory)
        'soato_code': to_text(get_value(api_response, 'soato_code')),
        'soato_description': to_text(get_value(api_response, 'soato_name')),

        # Contact information
        'region': to_text(get_value(api_response, 'region')),
        'city': to_text(get_value(api_response, 'area')),
        'street_address': to_text(get_value(api_response, 'address')),
        'email': to_text(get_value(api_response, 'email')),
        'phone': to_text(phone),

        # Village/MFY
        'village_code': get_value(api_response, 'village_code'),
        'village_name': to_text(get_value(village_detail, 'name')),

        # Additional MyOrg fields
        'trust': trust_value,
        'score': score_value,
        'itpark': (itpark_value or 0) == 1,
        'is_bankrupt': (is_bankrupt_value or 0) == 1,
        'is_abuse_vat': (is_abuse_vat_value or 0) == 1,
        'is_large_taxpayer': (is_large_taxpayer_value or 0) == 1,
        'relevance_date': parse_date(get_value(api_response, 'relevance_date')),

        # Verification status (trust-based)
        'is_verified': trust_value is not None and trust_value > 0,
    }

    return company_data


def fetch_company_deals(company_id: int, base_url: Optional[str] = None, bearer_token: Optional[str] = None) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """
    Fetch company deals from MyOrg API by company_id

    Real API endpoint: GET /api/deal/{company_id}
    Auth header: token: Bearer {jwt_token}

    Args:
        company_id: Company ID from entity API
        base_url: Optional override for API base URL (uses secrets if not provided)
        bearer_token: Optional override for bearer token (uses secrets if not provided)

    Returns:
        (success, data, error_message) tuple
        - success: Boolean indicating if request was successful
        - data: Complete API response dictionary or None if failed
        - error_message: Error message if failed, None if successful
    """
    # Get config from secrets if not provided
    if not base_url or not bearer_token:
        config_url, config_token = get_myorg_config()
        base_url = base_url or config_url
        bearer_token = bearer_token or config_token

    if not base_url or not bearer_token:
        return False, None, "MyOrg API configuration not found in secrets"

    if not company_id:
        return False, None, "Company ID is required"

    try:
        # Construct API endpoint
        url = f"{base_url.rstrip('/')}/api/deal/{company_id}"

        headers = {
            "token": f"Bearer {bearer_token}",
            "accept": "application/json"
        }

        print(f"Fetching deals data from MyOrg for company_id: {company_id}")

        # Make API request with retry logic
        response = retry_request(
            lambda: requests.get(url, headers=headers, timeout=30),
            f"deals (company_id: {company_id})",
            max_retries=10,
            retry_delay=2.0
        )

        # Check if request was successful
        if response.status_code == 200:
            data = response.json()
            print(f"Successfully fetched deals data for company_id: {company_id}")
            return True, data, None
        elif response.status_code == 404:
            return False, None, f"No deals found for company_id {company_id}"
        elif response.status_code == 401:
            return False, None, "Authentication failed - invalid bearer token"
        elif response.status_code == 403:
            return False, None, "Access forbidden - insufficient permissions"
        else:
            return False, None, f"API request failed with status {response.status_code}: {response.text}"

    except requests.exceptions.RequestException as e:
        return False, None, f"API request error: {str(e)}"
    except Exception as e:
        return False, None, f"Unexpected error: {str(e)}"


def fetch_court_cases(inn: str, base_url: Optional[str] = None, bearer_token: Optional[str] = None) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """
    Fetch court cases from MyOrg API by INN

    Real API endpoint: GET /api/court/inn/{inn}
    Auth header: token: Bearer {jwt_token}

    Args:
        inn: Tax Identification Number (9 digits)
        base_url: Optional override for API base URL (uses secrets if not provided)
        bearer_token: Optional override for bearer token (uses secrets if not provided)

    Returns:
        (success, data, error_message) tuple
    """
    # Get config from secrets if not provided
    if not base_url or not bearer_token:
        config_url, config_token = get_myorg_config()
        base_url = base_url or config_url
        bearer_token = bearer_token or config_token

    if not base_url or not bearer_token:
        return False, None, "MyOrg API configuration not found in secrets"

    if not inn:
        return False, None, "INN (Tax ID) is required"

    # Clean INN
    inn = str(inn).replace(' ', '').strip()

    # Validate INN format (9 digits)
    if not inn.isdigit() or len(inn) != 9:
        return False, None, f"Invalid INN format: {inn}. INN must be exactly 9 digits"

    try:
        url = f"{base_url.rstrip('/')}/api/court/inn/{inn}?offset=0&limit=100"

        headers = {
            "token": f"Bearer {bearer_token}",
            "accept": "application/json"
        }

        print(f"Fetching court cases from MyOrg for INN: {inn}")

        # Make API request with retry logic
        response = retry_request(
            lambda: requests.get(url, headers=headers, timeout=30),
            f"court_cases (INN: {inn})",
            max_retries=10,
            retry_delay=2.0
        )

        if response.status_code == 200:
            data = response.json()
            print(f"Successfully fetched court cases for INN: {inn}")
            return True, data, None
        elif response.status_code == 404:
            return False, None, f"No court cases found for INN {inn}"
        elif response.status_code == 401:
            return False, None, "Authentication failed - invalid bearer token"
        elif response.status_code == 403:
            return False, None, "Access forbidden - insufficient permissions"
        else:
            return False, None, f"API request failed with status {response.status_code}: {response.text}"

    except requests.exceptions.RequestException as e:
        return False, None, f"API request error: {str(e)}"
    except Exception as e:
        return False, None, f"Unexpected error: {str(e)}"


def fetch_company_connections(company_id: int, base_url: Optional[str] = None, bearer_token: Optional[str] = None) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """
    Fetch company connections from MyOrg API by company_id

    Real API endpoint: GET /api/entity/{company_id}/connection
    Auth header: token: Bearer {jwt_token}

    Args:
        company_id: Company ID from entity API
        base_url: Optional override for API base URL (uses secrets if not provided)
        bearer_token: Optional override for bearer token (uses secrets if not provided)

    Returns:
        (success, data, error_message) tuple
    """
    # Get config from secrets if not provided
    if not base_url or not bearer_token:
        config_url, config_token = get_myorg_config()
        base_url = base_url or config_url
        bearer_token = bearer_token or config_token

    if not base_url or not bearer_token:
        return False, None, "MyOrg API configuration not found in secrets"

    if not company_id:
        return False, None, "Company ID is required"

    try:
        url = f"{base_url.rstrip('/')}/api/entity/{company_id}/connection"

        headers = {
            "token": f"Bearer {bearer_token}",
            "accept": "application/json"
        }

        print(f"Fetching connections data from MyOrg for company_id: {company_id}")

        # Make API request with retry logic
        response = retry_request(
            lambda: requests.get(url, headers=headers, timeout=30),
            f"connections (company_id: {company_id})",
            max_retries=10,
            retry_delay=2.0
        )

        if response.status_code == 200:
            data = response.json()
            print(f"Successfully fetched connections data for company_id: {company_id}")
            return True, data, None
        elif response.status_code == 404:
            return False, None, f"No connections found for company_id {company_id}"
        elif response.status_code == 401:
            return False, None, "Authentication failed - invalid bearer token"
        elif response.status_code == 403:
            return False, None, "Access forbidden - insufficient permissions"
        else:
            return False, None, f"API request failed with status {response.status_code}: {response.text}"

    except requests.exceptions.RequestException as e:
        return False, None, f"API request error: {str(e)}"
    except Exception as e:
        return False, None, f"Unexpected error: {str(e)}"


def fetch_company_liabilities(company_id: int, base_url: Optional[str] = None, bearer_token: Optional[str] = None) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """
    Fetch company liabilities from MyOrg API by company_id

    Real API endpoint: GET /api/entity/liability/{company_id}
    Auth header: token: Bearer {jwt_token}

    Args:
        company_id: Company ID from entity API
        base_url: Optional override for API base URL (uses secrets if not provided)
        bearer_token: Optional override for bearer token (uses secrets if not provided)

    Returns:
        (success, data, error_message) tuple
    """
    # Get config from secrets if not provided
    if not base_url or not bearer_token:
        config_url, config_token = get_myorg_config()
        base_url = base_url or config_url
        bearer_token = bearer_token or config_token

    if not base_url or not bearer_token:
        return False, None, "MyOrg API configuration not found in secrets"

    if not company_id:
        return False, None, "Company ID is required"

    try:
        url = f"{base_url.rstrip('/')}/api/entity/liability/{company_id}"

        headers = {
            "token": f"Bearer {bearer_token}",
            "accept": "application/json"
        }

        print(f"Fetching liabilities data from MyOrg for company_id: {company_id}")

        # Make API request with retry logic
        response = retry_request(
            lambda: requests.get(url, headers=headers, timeout=30),
            f"liabilities (company_id: {company_id})",
            max_retries=10,
            retry_delay=2.0
        )

        if response.status_code == 200:
            data = response.json()
            print(f"Successfully fetched liabilities data for company_id: {company_id}")
            return True, data, None
        elif response.status_code == 404:
            # 404 is OK for liabilities - just means no liabilities
            return True, {"total": 0}, None
        elif response.status_code == 401:
            return False, None, "Authentication failed - invalid bearer token"
        elif response.status_code == 403:
            return False, None, "Access forbidden - insufficient permissions"
        else:
            return False, None, f"API request failed with status {response.status_code}: {response.text}"

    except requests.exceptions.RequestException as e:
        return False, None, f"API request error: {str(e)}"
    except Exception as e:
        return False, None, f"Unexpected error: {str(e)}"


def fetch_company_licenses(company_id: int, base_url: Optional[str] = None, bearer_token: Optional[str] = None) -> Tuple[bool, Optional[List[Dict[str, Any]]], Optional[str]]:
    """
    Fetch company licenses from MyOrg API by company_id

    Real API endpoint: GET /api/entity/license/{company_id}
    Auth header: token: Bearer {jwt_token}

    Args:
        company_id: Company ID from entity API
        base_url: Optional override for API base URL (uses secrets if not provided)
        bearer_token: Optional override for bearer token (uses secrets if not provided)

    Returns:
        (success, data, error_message) tuple
        Note: data is a LIST of licenses, not a dict
    """
    # Get config from secrets if not provided
    if not base_url or not bearer_token:
        config_url, config_token = get_myorg_config()
        base_url = base_url or config_url
        bearer_token = bearer_token or config_token

    if not base_url or not bearer_token:
        return False, None, "MyOrg API configuration not found in secrets"

    if not company_id:
        return False, None, "Company ID is required"

    try:
        url = f"{base_url.rstrip('/')}/api/entity/license/{company_id}"

        headers = {
            "token": f"Bearer {bearer_token}",
            "accept": "application/json"
        }

        print(f"Fetching licenses data from MyOrg for company_id: {company_id}")

        # Make API request with retry logic
        response = retry_request(
            lambda: requests.get(url, headers=headers, timeout=30),
            f"licenses (company_id: {company_id})",
            max_retries=10,
            retry_delay=2.0
        )

        if response.status_code == 200:
            data = response.json()
            print(f"Successfully fetched licenses data for company_id: {company_id}")
            return True, data, None
        elif response.status_code == 404:
            # 404 is OK for licenses - just means no licenses
            return True, [], None
        elif response.status_code == 401:
            return False, None, "Authentication failed - invalid bearer token"
        elif response.status_code == 403:
            return False, None, "Access forbidden - insufficient permissions"
        else:
            return False, None, f"API request failed with status {response.status_code}: {response.text}"

    except requests.exceptions.RequestException as e:
        return False, None, f"API request error: {str(e)}"
    except Exception as e:
        return False, None, f"Unexpected error: {str(e)}"


def fetch_founder_connections(director_uuid: str, base_url: Optional[str] = None, bearer_token: Optional[str] = None) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """
    Fetch founder connections from MyOrg API by director UUID

    Real API endpoint: GET /api/entity/connection/founder/{director_uuid}
    Auth header: token: Bearer {jwt_token}

    Args:
        director_uuid: UUID of the director/founder
        base_url: Optional override for API base URL (uses secrets if not provided)
        bearer_token: Optional override for bearer token (uses secrets if not provided)

    Returns:
        (success, data, error_message) tuple
    """
    # Get config from secrets if not provided
    if not base_url or not bearer_token:
        config_url, config_token = get_myorg_config()
        base_url = base_url or config_url
        bearer_token = bearer_token or config_token

    if not base_url or not bearer_token:
        return False, None, "MyOrg API configuration not found in secrets"

    if not director_uuid:
        return False, None, "Director UUID is required"

    try:
        url = f"{base_url.rstrip('/')}/api/entity/connection/founder/{director_uuid}"

        headers = {
            "token": f"Bearer {bearer_token}",
            "accept": "application/json"
        }

        print(f"Fetching founder connections from MyOrg for director UUID: {director_uuid}")

        # Make API request with retry logic
        response = retry_request(
            lambda: requests.get(url, headers=headers, timeout=30),
            f"founder_connections (UUID: {director_uuid})",
            max_retries=10,
            retry_delay=2.0
        )

        if response.status_code == 200:
            data = response.json()
            print(f"Successfully fetched founder connections for UUID: {director_uuid}")
            return True, data, None
        elif response.status_code == 404:
            # 404 is OK for founder connections - just means no connections
            return True, [], None
        elif response.status_code == 401:
            return False, None, "Authentication failed - invalid bearer token"
        elif response.status_code == 403:
            return False, None, "Access forbidden - insufficient permissions"
        else:
            return False, None, f"API request failed with status {response.status_code}: {response.text}"

    except requests.exceptions.RequestException as e:
        return False, None, f"API request error: {str(e)}"
    except Exception as e:
        return False, None, f"Unexpected error: {str(e)}"


def fetch_company_rating(company_id: int, base_url: Optional[str] = None, bearer_token: Optional[str] = None) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """
    Fetch company rating from MyOrg API by company_id

    Real API endpoint: GET /api/entity/rating/{company_id}
    Auth header: token: Bearer {jwt_token}

    Args:
        company_id: Company ID from entity API
        base_url: Optional override for API base URL (uses secrets if not provided)
        bearer_token: Optional override for bearer token (uses secrets if not provided)

    Returns:
        (success, data, error_message) tuple
    """
    # Get config from secrets if not provided
    if not base_url or not bearer_token:
        config_url, config_token = get_myorg_config()
        base_url = base_url or config_url
        bearer_token = bearer_token or config_token

    if not base_url or not bearer_token:
        return False, None, "MyOrg API configuration not found in secrets"

    if not company_id:
        return False, None, "Company ID is required"

    try:
        url = f"{base_url.rstrip('/')}/api/entity/rating/{company_id}"

        headers = {
            "token": f"Bearer {bearer_token}",
            "accept": "application/json"
        }

        print(f"Fetching rating data from MyOrg for company_id: {company_id}")

        # Make API request with retry logic
        response = retry_request(
            lambda: requests.get(url, headers=headers, timeout=30),
            f"rating (company_id: {company_id})",
            max_retries=10,
            retry_delay=2.0
        )

        if response.status_code == 200:
            data = response.json()
            print(f"Successfully fetched rating data for company_id: {company_id}")
            return True, data, None
        elif response.status_code == 404:
            # 404 is OK for rating - just means no rating available
            return True, {}, None
        elif response.status_code == 401:
            return False, None, "Authentication failed - invalid bearer token"
        elif response.status_code == 403:
            return False, None, "Access forbidden - insufficient permissions"
        else:
            return False, None, f"API request failed with status {response.status_code}: {response.text}"

    except requests.exceptions.RequestException as e:
        return False, None, f"API request error: {str(e)}"
    except Exception as e:
        return False, None, f"Unexpected error: {str(e)}"


def fetch_company_collateral(inn: str, base_url: Optional[str] = None, bearer_token: Optional[str] = None) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """
    Fetch company collateral information from MyOrg API by INN

    Real API endpoint: GET /api/collateral/{inn}
    Auth header: token: Bearer {jwt_token}

    Args:
        inn: Tax Identification Number (9 digits)
        base_url: Optional override for API base URL (uses secrets if not provided)
        bearer_token: Optional override for bearer token (uses secrets if not provided)

    Returns:
        (success, data, error_message) tuple
    """
    # Get config from secrets if not provided
    if not base_url or not bearer_token:
        config_url, config_token = get_myorg_config()
        base_url = base_url or config_url
        bearer_token = bearer_token or config_token

    if not base_url or not bearer_token:
        return False, None, "MyOrg API configuration not found in secrets"

    if not inn:
        return False, None, "INN (Tax ID) is required"

    # Clean INN
    inn = str(inn).replace(' ', '').strip()

    # Validate INN format (9 digits)
    if not inn.isdigit() or len(inn) != 9:
        return False, None, f"Invalid INN format: {inn}. INN must be exactly 9 digits"

    try:
        url = f"{base_url.rstrip('/')}/api/collateral/{inn}"

        headers = {
            "token": f"Bearer {bearer_token}",
            "accept": "application/json"
        }

        print(f"Fetching collateral data from MyOrg for INN: {inn}")

        # Make API request with retry logic
        response = retry_request(
            lambda: requests.get(url, headers=headers, timeout=30),
            f"collateral (INN: {inn})",
            max_retries=10,
            retry_delay=2.0
        )

        if response.status_code == 200:
            data = response.json()
            print(f"Successfully fetched collateral data for INN: {inn}")
            return True, data, None
        elif response.status_code == 404:
            # 404 is OK for collateral - just means no collateral data
            return True, {}, None
        elif response.status_code == 401:
            return False, None, "Authentication failed - invalid bearer token"
        elif response.status_code == 403:
            return False, None, "Access forbidden - insufficient permissions"
        else:
            return False, None, f"API request failed with status {response.status_code}: {response.text}"

    except requests.exceptions.RequestException as e:
        return False, None, f"API request error: {str(e)}"
    except Exception as e:
        return False, None, f"Unexpected error: {str(e)}"


def search_companies(query: str, tab: int = 1, base_url: Optional[str] = None, bearer_token: Optional[str] = None) -> Tuple[bool, Optional[List[Dict[str, Any]]], Optional[str], int]:
    """
    Search for companies using MyOrg API search endpoint

    Real API endpoint: GET /api/search?query={query}&tab={tab}
    Auth header: token: Bearer {jwt_token}

    Args:
        query: Search query (company name, INN, director name, etc.)
        tab: Tab number (1=Companies, 2=KYC, 3=Trademarks, etc.) Default is 1
        base_url: Optional override for API base URL (uses secrets if not provided)
        bearer_token: Optional override for bearer token (uses secrets if not provided)

    Returns:
        (success, results_list, error_message, total_count) tuple
        - success: Boolean indicating if request was successful
        - results_list: List of company search results or None if failed
        - error_message: Error message if failed, None if successful
        - total_count: Total number of results available
    """
    # Get config from secrets if not provided
    if not base_url or not bearer_token:
        config_url, config_token = get_myorg_config()
        base_url = base_url or config_url
        bearer_token = bearer_token or config_token
    
    if not base_url or not bearer_token:
        return False, None, "MyOrg API configuration not found in secrets", 0
    
    if not query or not query.strip():
        return False, None, "Search query is required", 0
    
    try:
        # Construct API endpoint
        url = f"{base_url.rstrip('/')}/api/search"
        
        # Query parameters
        params = {
            "query": query.strip(),
            "tab": tab
        }
        
        headers = {
            "token": f"Bearer {bearer_token}",
            "accept": "application/json"
        }
        
        print(f"Searching companies in MyOrg with query: {query}, tab: {tab}")
        
        # Make API request with timeout
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        # Check if request was successful
        if response.status_code == 200:
            data = response.json()
            
            # MyOrg search API returns nested structure: entity -> name -> rows
            total_count = 0
            if isinstance(data, dict):
                # Navigate the nested structure
                if 'entity' in data:
                    entity = data['entity']
                    if isinstance(entity, dict) and 'name' in entity:
                        name_data = entity['name']
                        if isinstance(name_data, dict) and 'rows' in name_data:
                            rows = name_data['rows']
                            total_count = name_data.get('total', len(rows))
                            print(f"Successfully searched companies: {len(rows)} results, total: {total_count}")
                            return True, rows, None, total_count
                
                # Fallback: try other common structures
                if 'results' in data:
                    results = data['results']
                    total_count = data.get('total', len(results))
                    return True, results, None, total_count
                elif 'data' in data:
                    results = data['data']
                    total_count = data.get('total', len(results))
                    return True, results, None, total_count
                elif 'rows' in data:
                    rows = data['rows']
                    total_count = data.get('total', len(rows))
                    return True, rows, None, total_count
            
            # If it's already a list, return it
            if isinstance(data, list):
                print(f"Successfully searched companies: {len(data)} results")
                return True, data, None, len(data)
            
            # Last resort: wrap in list
            print(f"Successfully searched companies: unknown structure")
            return True, [data] if data else [], None, 1 if data else 0
                
        elif response.status_code == 404:
            return True, [], None, 0  # No results found is not an error
        elif response.status_code == 401:
            return False, None, "Authentication failed - invalid bearer token", 0
        elif response.status_code == 403:
            return False, None, "Access forbidden - insufficient permissions", 0
        else:
            return False, None, f"API request failed with status {response.status_code}: {response.text}", 0
    
    except requests.exceptions.Timeout:
        return False, None, "API request timed out (30s)", 0
    except requests.exceptions.ConnectionError:
        return False, None, "Failed to connect to MyOrg API - check network connection", 0
    except requests.exceptions.RequestException as e:
        return False, None, f"API request error: {str(e)}", 0
    except Exception as e:
        return False, None, f"Unexpected error: {str(e)}", 0


def fetch_company_history(inn: str, base_url: Optional[str] = None, bearer_token: Optional[str] = None) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """
    Fetch company history from MyOrg API by INN
    
    Real API endpoint: GET /api/entity/inn/{inn}/history
    Auth header: token: Bearer {jwt_token}
    
    Returns historical changes to company data with "before" and "after" states.
    
    Args:
        inn: Tax Identification Number (9 digits)
        base_url: Optional override for API base URL (uses secrets if not provided)
        bearer_token: Optional override for bearer token (uses secrets if not provided)
    
    Returns:
        (success, data, error_message) tuple
        - success: Boolean indicating if request was successful
        - data: API response with rows array of history records, each containing:
                {before: {...}, after: {...}, created_at: "timestamp"}
        - error_message: Error message if failed, None if successful
    """
    # Get config from secrets if not provided
    if not base_url or not bearer_token:
        config_url, config_token = get_myorg_config()
        base_url = base_url or config_url
        bearer_token = bearer_token or config_token
    
    if not base_url or not bearer_token:
        return False, None, "MyOrg API configuration not found in secrets"
    
    if not inn:
        return False, None, "INN (Tax ID) is required"
    
    # Clean INN (remove any whitespace)
    inn = str(inn).replace(' ', '').strip()
    
    # Validate INN format (9 digits)
    if not inn.isdigit() or len(inn) != 9:
        return False, None, f"Invalid INN format: {inn}. INN must be exactly 9 digits"
    
    try:
        url = f"{base_url.rstrip('/')}/api/entity/inn/{inn}/history"

        headers = {
            "token": f"Bearer {bearer_token}",
            "accept": "application/json"
        }

        print(f"Fetching history data from MyOrg for INN: {inn}")

        # Make API request with retry logic
        response = retry_request(
            lambda: requests.get(url, headers=headers, timeout=30),
            f"history (INN: {inn})",
            max_retries=10,
            retry_delay=2.0
        )

        if response.status_code == 200:
            data = response.json()
            print(f"Successfully fetched history data for INN: {inn}")
            return True, data, None
        elif response.status_code == 404:
            return False, None, f"No history found for INN {inn}"
        elif response.status_code == 401:
            return False, None, "Authentication failed - invalid bearer token"
        elif response.status_code == 403:
            return False, None, "Access forbidden - insufficient permissions"
        else:
            return False, None, f"API request failed with status {response.status_code}: {response.text}"

    except requests.exceptions.RequestException as e:
        return False, None, f"API request error: {str(e)}"
    except Exception as e:
        return False, None, f"Unexpected error: {str(e)}"


# Backward compatibility - keep old function name
fetch_company_info = fetch_company_by_inn
parse_company_data = parse_myorg_response
