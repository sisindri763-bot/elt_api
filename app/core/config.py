import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".env"))

DB_HOST     = os.getenv("CENTRAL_DB_HOST") or os.getenv("DB_HOST", "localhost")
DB_PORT     = int(os.getenv("CENTRAL_DB_PORT") or os.getenv("DB_PORT", "3306"))
DB_USER     = os.getenv("CENTRAL_DB_USER") or os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("CENTRAL_DB_PASSWORD") or os.getenv("DB_PASSWORD", "")
DB_NAME     = os.getenv("CENTRAL_DB_NAME") or os.getenv("DB_NAME", "metadata")
