"""
auth_routes.py
==============
Authentication routes for the Market Cap Consolidation Tool.
- POST /api/auth/signup  — register a new user
- POST /api/auth/signin  — sign in and get a JWT token
- GET  /api/auth/verify  — verify a JWT token (used by frontend on page load)

Users are stored in the `users` collection of the existing `Stocks` MongoDB database.
Schema per user document:
  {
    "first_name": str,
    "last_name":  str,
    "email":      str,  (unique, lowercased index)
    "password":   str,  (bcrypt hash)
    "created_at": str   (ISO timestamp)
  }

This module does NOT touch any existing collections or application logic.
"""

from flask import Blueprint, request, jsonify
import bcrypt
import jwt
import os
from datetime import datetime, timezone, timedelta

auth_bp = Blueprint('auth', __name__, url_prefix='/api/auth')

# -----------------------------------------------------------------------
# JWT secret — use env var in production, fall back to a dev default
# -----------------------------------------------------------------------
JWT_SECRET = os.getenv('JWT_SECRET', 'market-cap-tool-jwt-secret-key-2026')
JWT_ALGORITHM = 'HS256'
JWT_EXPIRY_HOURS = 72  # 3 days

# `users_collection` is injected at app startup via init_auth(db)
users_collection = None


def init_auth(db):
    """
    Call this from app.py once MongoDB is connected.
    Receives the `db` object and sets up the users collection + indexes.
    """
    global users_collection
    if db is None:
        return
    users_collection = db['users']
    # Unique index on email so duplicate accounts are rejected at the DB level
    try:
        users_collection.create_index('email', unique=True, name='email_unique_idx')
    except Exception as exc:
        print(f'[auth] Index creation warning: {exc}')
    print('[auth] ✅ Users collection ready')


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

def _make_token(user_doc):
    """Create a signed JWT for a user."""
    payload = {
        'user_id': str(user_doc['_id']),
        'email':      user_doc['email'],
        'first_name': user_doc['first_name'],
        'last_name':  user_doc['last_name'],
        'exp': datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRY_HOURS)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def _decode_token(token):
    """Decode and verify a JWT. Returns payload dict or raises."""
    return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])


def _get_token_from_request():
    """Extract bearer token from Authorization header."""
    auth_header = request.headers.get('Authorization', '')
    if auth_header.startswith('Bearer '):
        return auth_header[7:]
    return None


# -----------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------

@auth_bp.route('/signup', methods=['POST'])
def signup():
    """Register a new user."""
    if users_collection is None:
        return jsonify({'error': 'Database not connected'}), 503

    data = request.get_json(force=True) or {}
    first_name = (data.get('first_name') or '').strip()
    last_name  = (data.get('last_name')  or '').strip()
    email      = (data.get('email')      or '').strip().lower()
    password   = (data.get('password')   or '').strip()
    confirm_pw = (data.get('confirm_password') or '').strip()

    # Validation
    if not first_name:
        return jsonify({'error': 'First name is required'}), 400
    if not last_name:
        return jsonify({'error': 'Last name is required'}), 400
    if not email or '@' not in email:
        return jsonify({'error': 'A valid email is required'}), 400
    if len(password) < 6:
        return jsonify({'error': 'Password must be at least 6 characters'}), 400
    if password != confirm_pw:
        return jsonify({'error': 'Passwords do not match'}), 400

    # Check duplicate email
    if users_collection.find_one({'email': email}):
        return jsonify({'error': 'An account with this email already exists'}), 409

    # Hash password
    pw_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

    user_doc = {
        'first_name': first_name,
        'last_name':  last_name,
        'email':      email,
        'password':   pw_hash,
        'created_at': datetime.now(timezone.utc).isoformat()
    }

    result = users_collection.insert_one(user_doc)
    user_doc['_id'] = result.inserted_id

    token = _make_token(user_doc)

    return jsonify({
        'message': 'Account created successfully',
        'token': token,
        'user': {
            'first_name': first_name,
            'last_name':  last_name,
            'email':      email
        }
    }), 201


@auth_bp.route('/signin', methods=['POST'])
def signin():
    """Authenticate a user and return a JWT."""
    if users_collection is None:
        return jsonify({'error': 'Database not connected'}), 503

    data     = request.get_json(force=True) or {}
    email    = (data.get('email')    or '').strip().lower()
    password = (data.get('password') or '').strip()

    if not email or not password:
        return jsonify({'error': 'Email and password are required'}), 400

    user = users_collection.find_one({'email': email})
    if not user:
        return jsonify({'error': 'Invalid email or password'}), 401

    if not bcrypt.checkpw(password.encode('utf-8'), user['password']):
        return jsonify({'error': 'Invalid email or password'}), 401

    token = _make_token(user)

    return jsonify({
        'message': 'Signed in successfully',
        'token': token,
        'user': {
            'first_name': user['first_name'],
            'last_name':  user['last_name'],
            'email':      user['email']
        }
    }), 200


@auth_bp.route('/verify', methods=['GET'])
def verify():
    """Verify a JWT token. Used by the frontend on page load."""
    token = _get_token_from_request()
    if not token:
        return jsonify({'error': 'No token provided'}), 401

    try:
        payload = _decode_token(token)
        return jsonify({
            'valid': True,
            'user': {
                'first_name': payload.get('first_name', ''),
                'last_name':  payload.get('last_name', ''),
                'email':      payload.get('email', '')
            }
        }), 200
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401
