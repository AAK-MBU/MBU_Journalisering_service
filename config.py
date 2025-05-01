"""
Stores configuration settings for the service.
"""

# Fetch interval in seconds (e.g., 5 minutes)
FETCH_INTERVAL = 300

# Heartbeat interval in seconds (e.g., 1 minute)
HEARTBEAT_INTERVAL = 60
SERVICE_CHECK_INTERVAL = 60
MAX_WORKERS = 4

# Base API URL
BASE_API_URL = "https://selvbetjening.aarhuskommune.dk/da"

# Log database
LOG_DB = "journalizing.journalize_log"
LOG_CONTEXT = "Journalize service"

# Forms to handle
HANDLE_FORMS = [
    "...",
]

# Environment (prod or test)
ENV = "..."

# Path to pythonservice within venv
PATH_TO_PYTHONSERVICE = R"..."
