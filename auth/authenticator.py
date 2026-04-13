import streamlit as st
import streamlit_authenticator as stauth
from streamlit_authenticator.utilities.exceptions import CredentialsError, ForgotError, LoginError, RegisterError, ResetError, UpdateError
import yaml
from yaml.loader import SafeLoader
import os


def load_config():
    """Load authentication configuration from users.yaml file."""
    try:
        # Load configuration from YAML file
        config_path = os.path.join(os.path.dirname(__file__), '..', '.streamlit', 'users.yaml')
        
        with open(config_path, 'r', encoding='utf-8') as file:
            config = yaml.load(file, Loader=SafeLoader)
        return config
    except Exception as e:
        st.error(f"Error loading authentication config: {e}")
        return None


def get_authenticator():
    """Get configured authenticator instance with caching."""
    if 'authenticator' not in st.session_state:
        config = load_config()
        if config is None:
            return None

        try:
            # Passwords are already hashed in the YAML file, so use them directly
            st.session_state['authenticator'] = stauth.Authenticate(
                config['credentials'],
                config['cookie']['name'],
                config['cookie']['key'],
                config['cookie']['expiry_days']
            )
        except Exception as e:
            st.error(f"Error creating authenticator: {e}")
            return None
    
    return st.session_state['authenticator']


def show_login_if_needed():
    """
    Show login form if user is not authenticated.
    Returns True if user is authenticated, False otherwise.
    If not authenticated, displays login form and handles authentication process.
    """
    # Initialize authentication status in session state
    if 'authentication_status' not in st.session_state:
        st.session_state['authentication_status'] = None
    if 'name' not in st.session_state:
        st.session_state['name'] = None
    if 'username' not in st.session_state:
        st.session_state['username'] = None
    
    # Check if user is already authenticated
    if st.session_state['authentication_status']:
        return True
    
    authenticator = get_authenticator()
    if authenticator is None:
        st.error("Authentication system not available. Please check configuration.")
        return False
    
    # Show login form if not authenticated
    st.title("🔐 Login")
    
    try:
        authenticator.login(
            fields={
                'Form name': 'Login',
                'Username': 'Username',
                'Password': 'Password',
                'Login': 'Login'
            },
            location='main'
        )
    except LoginError as e:
        st.error(f"Login error: {e}")
    except Exception as e:
        st.error(f"Authentication error: {e}")
    
    # Check authentication status
    if st.session_state['authentication_status'] is False:
        st.error('Username/password is incorrect')
        return False
    elif st.session_state['authentication_status'] is None:
        st.warning('Please enter your username and password')
        return False
    elif st.session_state['authentication_status']:
        return True
    
    return False


def show_logout_button():
    authenticator = get_authenticator()
    if authenticator is None:
        return
    
    try:
        with st.sidebar:
            # Logout button using the authenticator's logout method
            authenticator.logout('Logout', 'sidebar')
    except Exception as e:
        st.error(f"Error showing logout button: {e}")


def get_current_user():
    """Get current authenticated user information."""
    if st.session_state.get('authentication_status'):
        return {
            'name': st.session_state.get('name'),
            'username': st.session_state.get('username'),
            'authenticated': True
        }
    return {
        'name': None,
        'username': None,
        'authenticated': False
    }


def check_authentication():
    return st.session_state.get('authentication_status', False)


def protect_page():
    """
    Protect a page by requiring authentication.
    Call this at the top of each page script.
    """
    if not check_authentication():
        st.title("🔐 Access Denied")
        st.error("You must be logged in to access this page.")
        st.info("Please go to the [Login](/) to log in.")
        st.stop()