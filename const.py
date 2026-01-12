import os

DOWNLOAD_EMAIL_WORKER_TIMEOUT_SECONDS = float(os.getenv('DOWNLOAD_EMAIL_WORKER_TIMEOUT_SECONDS', 1200))  # Default to 20 minutes if not set