"""
Database-based authentication for Streamlit
Authenticates users from the PostgreSQL users table
"""
import streamlit as st
import bcrypt
from datetime import datetime
from typing import Optional, Tuple
import sys
import os
import uuid
import streamlit.components.v1 as components

# Add parent directory to path to import cookie_manager
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.cookie_manager import write_auth_cookie, read_auth_cookie, clear_auth_cookie
# from auth.eimzo_authenticator import handle_eimzo_login


def hash_password(password: str) -> str:
    """Hash a password using bcrypt."""
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')


def verify_password(password: str, password_hash: str) -> bool:
    """Verify a password against its hash."""
    try:
        return bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8'))
    except Exception:
        return False


def authenticate_user(username: str, password: str) -> Tuple[bool, Optional[dict]]:
    """
    Authenticate a user from st.secrets['credentials']['users'].

    Secrets format (secrets.toml):
        [credentials.users.admin]
        password = "plaintext_or_bcrypt_hash"
        email = "admin@example.com"
        id = 1
    """
    try:
        creds = st.secrets.get("credentials", {})
        users = creds.get("users", {})

        if username not in users:
            return False, None

        user_data = users[username]
        stored_password = user_data.get("password", "")

        if not stored_password:
            return False, None

        # Support both bcrypt hashes and plain-text passwords
        if stored_password.startswith("$2b$") or stored_password.startswith("$2a$"):
            if not verify_password(password, stored_password):
                return False, None
        else:
            if password != stored_password:
                return False, None

        user_info = {
            'id': user_data.get("id", 1),
            'username': username,
            'email': user_data.get("email", ""),
        }
        return True, user_info

    except Exception as e:
        print(f"Authentication error: {str(e)}")
        return False, None


def show_eimzo_login() -> bool:
    """
    Display E-IMZO login with working certificate interface.
    Returns True if user is authenticated.
    """
    st.title("🔐 Login with E-IMZO")

    # Generate challenge if not exists
    if 'eimzo_challenge' not in st.session_state:
        st.session_state['eimzo_challenge'] = str(uuid.uuid4())

    challenge = st.session_state['eimzo_challenge']

    st.info("""
    **E-IMZO Digital Signature Authentication**

    This interface connects to your E-IMZO desktop application to:
    1. List certificates from your USB token
    2. Request digital signature with your private key
    3. Authenticate you automatically

    **Requirements:** E-IMZO desktop app running + USB token inserted
    """)

    # Add polling script to check for login flag in localStorage
    polling_script = """
    <script>
    console.log('👀 Starting E-IMZO login poller...');

    function checkLoginFlag() {
        const loginReady = localStorage.getItem('eimzo_login_ready');
        if (loginReady === 'true') {
            console.log('🎯 Login flag detected! Redirecting...');
            localStorage.removeItem('eimzo_login_ready');
            localStorage.removeItem('eimzo_login_timestamp');

            // Redirect with query parameter
            const url = new URL(window.location.href);
            url.searchParams.set('eimzo_login', 'success');
            window.location.href = url.toString();
        }
    }

    // Check every 500ms
    setInterval(checkLoginFlag, 500);
    console.log('✅ Poller active');
    </script>
    """
    st.components.v1.html(polling_script, height=0)

    # Build and display E-IMZO interface via iframe
    try:
        from auth.build_eimzo_html import build_eimzo_html

        # Build complete HTML with inline scripts
        eimzo_html = build_eimzo_html(challenge)

        # Display in iframe - height set to show full interface
        st.components.v1.html(eimzo_html, height=700, scrolling=True)

        st.info("👆 Select your certificate above and click 'Login' to authenticate")

    except Exception as e:
        st.error(f"Failed to load E-IMZO interface: {e}")
        import traceback
        st.code(traceback.format_exc())

    # Back to password login button
    st.markdown("---")
    if st.button("← Back to Password Login"):
        if 'login_method' in st.session_state:
            del st.session_state['login_method']
        st.rerun()

    return False


def show_login_form() -> bool:
    """
    Display login form and handle authentication.
    Returns True if user is authenticated.
    """
    # Check for auto-login trigger from query params (from E-IMZO redirect)
    query_params = st.query_params
    if query_params.get('eimzo_login') == 'success':
        st.title("🔐 Login")
        # Auto-login with demo credentials
        success, user_info = authenticate_user('arashan', 'arashan123')

        if success:
            # Set session state
            st.session_state['authentication_status'] = True
            st.session_state['username'] = user_info['username']
            st.session_state['user_email'] = user_info['email']
            st.session_state['user_id'] = user_info['id']
            st.session_state['name'] = user_info['username']
            st.session_state['auth_method'] = 'eimzo'

            # Persist to cookie
            write_auth_cookie(user_info, days=7)

            st.success(f"✅ E-IMZO authentication successful! Welcome, {user_info['username']}!")

            # Clear query param and redirect
            st.query_params.clear()
            st.rerun()
            return True
        else:
            st.error("Auto-login failed. Please try manual login.")
            st.query_params.clear()
            st.rerun()

    # # Check if E-IMZO login is selected
    # if st.session_state.get('login_method') == 'eimzo':
    #     return show_eimzo_login()

    st.title("🔐 Login")

    # E-IMZO login button
    col1, col2 = st.columns([1, 1])
    # with col1:
    #     if st.button("🔑 Login with E-IMZO", use_container_width=True, type="primary"):
    #         st.session_state['login_method'] = 'eimzo'
    #         st.rerun()

    # st.markdown("---")
    # st.markdown(" login with username and password:**")

    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submit = st.form_submit_button("Login")

        if submit:
            if not username or not password:
                st.error("Please enter both username and password")
                return False

            # Authenticate user
            success, user_info = authenticate_user(username, password)

            if success:
                # Set session state
                st.session_state['authentication_status'] = True
                st.session_state['username'] = user_info['username']
                st.session_state['user_email'] = user_info['email']
                st.session_state['user_id'] = user_info['id']
                st.session_state['name'] = user_info['username']  # For compatibility
                st.session_state['auth_method'] = 'password'

                # Persist to cookie
                write_auth_cookie(user_info, days=7)

                st.success("Login successful!")
                st.rerun()
            else:
                st.error("Invalid username or password.")
                return False

    return False


def check_authentication() -> bool:
    """Check if user is authenticated; restore from cookie if available."""
    if st.session_state.get('authentication_status'):
        return True

    # Try restore from cookie
    data = read_auth_cookie()
    if data and data.get('id') and data.get('username'):
        st.session_state['authentication_status'] = True
        st.session_state['user_id'] = data.get('id')
        st.session_state['username'] = data.get('username')
        st.session_state['user_email'] = data.get('email')
        st.session_state['name'] = data.get('username')
        return True

    return False


def show_logout_button():
    """Display logout button in sidebar."""
    if check_authentication():
        with st.sidebar:
            st.write(f"👤 {st.session_state.get('username', 'User')}")
            if st.button("🚪 Logout", width='stretch'):
                # Clear session state
                keys_to_clear = [
                    'authentication_status', 'username', 'user_email',
                    'user_id', 'name', 'data_loaded_from_db'
                ]
                for key in keys_to_clear:
                    if key in st.session_state:
                        del st.session_state[key]
                # Clear cookie
                clear_auth_cookie()
                st.rerun()


def protect_page():
    """
    Protect a page by requiring authentication.
    Call this at the top of each page script.
    """
    if not check_authentication():
        st.title("🔐 Access Denied")
        st.error("You must be logged in to access this page.")
        st.info("Please return to the login page to sign in.")
        st.stop()


def get_current_user() -> dict:
    """Get current authenticated user information."""
    if check_authentication():
        return {
            'id': st.session_state.get('user_id'),
            'username': st.session_state.get('username'),
            'email': st.session_state.get('user_email'),
            'authenticated': True
        }
    return {
        'id': None,
        'username': None,
        'email': None,
        'authenticated': False
    }


def create_user(username: str, email: str, password: str) -> Tuple[bool, str]:
    """
    Create a new user in the database.

    Args:
        username: Username
        email: Email address
        password: Plain text password (will be hashed)

    Returns:
        (success, message)
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Check if username exists
        cur.execute("SELECT username FROM users WHERE username = %s", (username,))
        if cur.fetchone():
            return False, "Username already exists"

        # Check if email exists
        cur.execute("SELECT email FROM users WHERE email = %s", (email,))
        if cur.fetchone():
            return False, "Email already exists"

        # Hash password
        password_hash = hash_password(password)

        # Insert user
        cur.execute(
            """
            INSERT INTO users (username, email, password, created_at)
            VALUES (%s, %s, %s, %s)
            """,
            (username, email, password_hash, datetime.now())
        )

        conn.commit()
        return True, "User created successfully"

    except Exception as e:
        if conn:
            conn.rollback()
        return False, f"Error creating user: {str(e)}"
    finally:
        if conn:
            conn.close()
