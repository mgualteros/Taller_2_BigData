import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
"""Este módulo contiene las configuraciones globales para el taller2 ETL."""

import configparser
from pathlib import Path
import typer
from etl import SUCCESS, CASSANDRA_CON_ERROR, MONGO_CON_ERROR, __app_name__

CONFIG_DIR_PATH = Path(typer.get_app_dir(__app_name__))
CONFIG_FILE_PATH = CONFIG_DIR_PATH / "config.ini"

MONGO_URI = "mongodb+srv://mgualterosg:uc2025@bigdata2025.0dy5p.mongodb.net/?retryWrites=true&w=majority&appName=BigData2025"
MONGO_DB = "Taller_2_BigData"
MONGO_COLLECTION = "Textos_sentencias"

CASSANDRA_HOSTS = ["127.0.0.1"]
CASSANDRA_KEYSPACE = "taller2"
BUNDLE_PATH = 
TOKEN_PATH = 

def init_app() -> int:
    """
    Inicializa el proyecto ETL creando el archivo y directorio de configuración.

    Returns:
        int: Código de éxito o error según la operación.
    """
    config_code = _init_config_file()
    if config_code != SUCCESS:
        return config_code
    
    database_code = _create_config_file()
    if database_code != SUCCESS:
        return database_code
    
    return SUCCESS


def _init_config_file() -> int:
    """
    Crea el directorio y archivo inicial de configuración si no existen.

    Returns:
        int: Código de éxito o error según la operación.
    """
    try:
        CONFIG_DIR_PATH.mkdir(exist_ok=True)
    except OSError:
        return CASSANDRA_CON_ERROR 
    
    try:
        CONFIG_FILE_PATH.touch(exist_ok=True)
    except OSError:
        return MONGO_CON_ERROR
    
    return SUCCESS


def _create_config_file() -> int:
    """
    Escribe los parámetros de configuración que están en el archivo 'config.ini'.

    Returns:
        int: Código de éxito o error según la operación.
    """
    config_parser = configparser.ConfigParser()

    # Configuración de MongoDB
    config_parser["MongoDB"] = {
        "uri": MONGO_URI,
        "db_name": MONGO_DB,
        "collection_name": MONGO_COLLECTION,
    }

    # Configuración de Cassandra
    config_parser["Cassandra"] = {
        "keyspace": CASSANDRA_KEYSPACE,
        "bundle_path": BUNDLE_PATH,
        "token_path": TOKEN_PATH,
    }
    
    try:
        with CONFIG_FILE_PATH.open("w") as file:
            config_parser.write(file)
    except OSError:
        return MONGO_CON_ERROR
    
    return SUCCESS