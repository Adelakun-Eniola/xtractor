from google.oauth2 import id_token
from google.auth.transport import requests
import os

def verify_google_token(token: str):
    try:
        idinfo = id_token.verify_oauth2_token(token, requests.Request(), os.getenv("GOOGLE_CLIENT_ID"))
        return idinfo  # contains email, name, picture, sub (user ID), etc.
    except Exception as e:
        print("Google token verification failed:", str(e))
        return None
