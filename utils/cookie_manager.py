import datetime
import os
import json
import time
import hmac
import hashlib
import base64
from typing import Optional, Dict, Any

import streamlit as st
import extra_streamlit_components as stx


def _get_secret_key() -> bytes:
    """Return a stable secret key for signing cookies.

    Looks up AUTH_COOKIE_SECRET in environment; falls back to a predictable
    development-only default so local reloads keep working.
    """
    secret = os.environ.get('AUTH_COOKIE_SECRET', 'dev-secret-not-for-prod')
    return secret.encode('utf-8')


def _get_cookie_manager() -> stx.CookieManager:
    if 'cookie_manager' not in st.session_state:
        st.session_state['cookie_manager'] = stx.CookieManager()
    return st.session_state['cookie_manager']


def _sign_payload(payload_b64: str) -> str:
    mac = hmac.new(_get_secret_key(), payload_b64.encode('utf-8'), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(mac).decode('utf-8').rstrip('=')


def _encode_token(data: Dict[str, Any]) -> str:
    payload_json = json.dumps(data, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
    payload_b64 = base64.urlsafe_b64encode(payload_json).decode('utf-8').rstrip('=')
    signature = _sign_payload(payload_b64)
    return f"{payload_b64}.{signature}"


def _decode_token(token: str) -> Optional[Dict[str, Any]]:
    try:
        payload_b64, signature = token.split('.', 1)
        expected_sig = _sign_payload(payload_b64)
        if not hmac.compare_digest(signature, expected_sig):
            return None
        # Pad base64
        padding = '=' * ((4 - len(payload_b64) % 4) % 4)
        payload_json = base64.urlsafe_b64decode(payload_b64 + padding)
        return json.loads(payload_json)
    except Exception:
        return None


COOKIE_NAME = 'auth_session'


def write_auth_cookie(user_info: Dict[str, Any], days: datetime.timedelta = datetime.timedelta(days=7)) -> None:
    data = {
        'id': user_info.get('id'),
        'username': user_info.get('username'),
        'email': user_info.get('email'),
        'iat': (time.time())
    }
    token = _encode_token(data)
    cm = _get_cookie_manager()
    # Convert timedelta (or int-like) to absolute datetime for cookie expiry
    try:
        if isinstance(days, datetime.timedelta):
            expires_at = datetime.datetime.utcnow() + days
        else:
            expires_at = datetime.datetime.utcnow() + datetime.timedelta(days=int(days))
    except Exception:
        expires_at = datetime.datetime.utcnow() + datetime.timedelta(days=7)
    cm.set(COOKIE_NAME, token, expires_at=expires_at)


def read_auth_cookie() -> Optional[Dict[str, Any]]:
    cm = _get_cookie_manager()
    token = cm.get(COOKIE_NAME)
    if not token:
        try:
            all_cookies = cm.get_all()
            token = (all_cookies or {}).get(COOKIE_NAME)
        except Exception:
            token = None
    if not token:
        return None
    data = _decode_token(token)
    return data


def clear_auth_cookie() -> None:
    cm = _get_cookie_manager()
    cm.delete(COOKIE_NAME)


def mount_cookie_manager() -> None:
    """Ensure the cookie manager component is mounted in the app.

    Call this early in the app before any auth checks to make sure cookies
    are available on the first render cycle.
    """
    _get_cookie_manager()

