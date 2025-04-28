import hashlib
import base64
import secrets
from datetime import datetime, timezone, timedelta

class AuthSOCWebService:
    def __init__(self, username, password):
        self.username = username
        self.password = password

    def create_timestamp(self):
        created = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        return created

    def create_expires(self):
        expires_timestamp = datetime.now(timezone.utc) + timedelta(minutes=1)
        expires = expires_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
        return expires

    def encode_nonce(self):
        nonce1 = secrets.token_bytes(16)
        nonce = base64.b64encode(nonce1).decode('utf-8')
        return nonce

    def calculate_digest(self, nonce, created, password):
        bcreated = bytes(created, "utf-8")
        bpassword = bytes(password, "utf-8")
        nonce2 = base64.b64decode(nonce)
        concat_bytes = nonce2 + bcreated + bpassword
        sha1_obj = hashlib.sha1()
        sha1_obj.update(concat_bytes)
        digest_bytes = sha1_obj.digest()
        encoded_digest = base64.b64encode(digest_bytes).decode("utf-8")
        return encoded_digest

    def calculate_username_token_id(self):
        credentials = self.username + self.password
        hashed_credentials = hashlib.sha1(credentials.encode()).hexdigest()
        username_token_id = "UsernameToken-" + hashed_credentials
        return username_token_id

    def calculate_timestamp_id(self, created, expires):
        timestamp_str = created + expires
        timestamp_hash = hashlib.sha1(timestamp_str.encode()).hexdigest()
        timestamp_id = "TS-" + timestamp_hash
        return timestamp_id