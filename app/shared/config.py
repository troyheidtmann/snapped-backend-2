"""
Configuration Module

This module manages application configuration settings and environment
variables for various services.

Features:
- Environment loading
- API credentials
- Email settings
- Service tokens
- SSL certificates

Data Model:
- API keys
- Tokens
- Credentials
- Server config
- Email settings

Security:
- Secure storage
- Token management
- SSL validation
- Access control
- Env isolation

Dependencies:
- certifi for SSL
- os for env
- dotenv for loading

Author: Snapped Development Team
"""

import certifi
import os
from dotenv import load_dotenv

load_dotenv()

# Social Blade Configuration
SOCIALBLADE_CLIENT_ID = os.getenv('SOCIALBLADE_CLIENT_ID')
SOCIALBLADE_TOKEN = os.getenv('SOCIALBLADE_TOKEN')

# Email Configuration
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USERNAME = "admin@snapped.cc"
SMTP_PASSWORD = "myrg othw qxlp aohb"
FROM_EMAIL = "Snapped Admin <admin@snapped.cc>"

# 12Labs Configuration
TWELVELABS_API_KEY = "tlk_2Q2CT173542GEK2CR57HP1M09VBZ"
