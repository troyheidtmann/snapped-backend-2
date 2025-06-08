from os import getenv

# Social Blade API credentials
SOCIALBLADE_CLIENT_ID = "cli_42cccd8a9a67eebd25e570c9"
SOCIALBLADE_ACCESS_TOKEN = "81b253eab177fc2018405546a0dff242e5b7063d3b0c123986d08c8ee0e52d69643082435b9fec00ad91503addb877270df0788cf006a1921a0e3628ecbfd1ea"

# Validate credentials are set
if not SOCIALBLADE_CLIENT_ID or not SOCIALBLADE_ACCESS_TOKEN:
    raise ValueError("Social Blade API credentials not found in environment") 