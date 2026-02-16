import os

DOWNLOAD_EMAIL_WORKER_TIMEOUT_SECONDS = int(os.getenv('DOWNLOAD_EMAIL_WORKER_TIMEOUT_SECONDS', 1200))  # Default to 20 minutes if not set