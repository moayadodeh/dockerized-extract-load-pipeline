from typing import Dict, Union
from dotenv import load_dotenv
import os

load_dotenv('config/.env')


MAIN_DIR = os.getenv('MAIN_DIR')

CODE_DIR = MAIN_DIR + 'code'
DATA_DIR = MAIN_DIR + 'data'

MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_DB = os.getenv('MYSQL_DB')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_PORT = os.getenv('MYSQL_PORT')

POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')



def get_settings() -> Dict[str, Union[str,int, Dict[str, str]]]:
    """Get the application settings as a dictionary.

    Returns:
        Dict[str, Union[str, Dict[str, str]]]: The application settings.
    """
    settings = {
        name.lower(): 
        value if not isinstance(value, dict) else value
        for name, value in globals().items()
        if name.isupper() and not name.startswith("__")
    }
    return settings


