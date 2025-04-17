import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from typing import Any, Dict
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
from datetime import datetime
from cassandra.util import Date
from etl.config import BUNDLE_PATH, TOKEN_PATH, CASSANDRA_KEYSPACE
from etl import CASSANDRA_CON_ERROR, SEARCH_ERROR, ERRORS

class DatabaseHandlerCassandra:
    def __init__(self):
        """
        Inicializa la conexión a Cassandra leyendo las variables desde config.py.
        """
        try:
            cloud_config = {"secure_connect_bundle": BUNDLE_PATH}
            with open(TOKEN_PATH) as f:
                secrets = json.load(f)

            auth_provider = PlainTextAuthProvider(
                secrets["clientId"], secrets["secret"]
            )
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)

            self.session = cluster.connect()
            self.session.set_keyspace(CASSANDRA_KEYSPACE)

        except Exception as e:
            raise RuntimeError(f"{ERRORS[CASSANDRA_CON_ERROR]}: {e}")

    def fetch_from_cassandra(self, path_cleaned: str) -> Dict[str, Any]:
        """
        Busca datos en Cassandra basados en el id_doc (path_cleaned) y consolida los resultados.

        Args:
            path_cleaned (str): El identificador limpio del archivo (id_doc).

        Returns:
            Dict[str, Any]: Diccionario con resumen, entidad (lista) y fecha consolidados.
        """
        query = "SELECT resumen, entidad, fecha FROM taller2.entidad_por_doc WHERE id_doc = %s ALLOW FILTERING"
        try:
            rows = self.session.execute(query, (path_cleaned,))
            if not rows:
                raise ValueError(f"No se encontraron resultados para id_doc='{path_cleaned}'")
            entidad_list = [] 
            resumen = ""
            fecha = None

            for row in rows:
                if row.entidad:
                    entidad_list.append(row.entidad)
                if not resumen:
                    resumen = row.resumen
                if not fecha:
                    if isinstance(row.fecha, Date): 
                        fecha = datetime.strptime(str(row.fecha), "%Y-%m-%d")
            return {
                "Resumen": resumen,
                "Entidad": entidad_list,
                "Fecha": fecha
            }
        except Exception as e:
            raise RuntimeError(f"{ERRORS[SEARCH_ERROR]}: {e}")
        
    def close_connection(self):
        """
        Cierra la conexión activa con Cassandra.
        """
        try:
            if self.session:
                self.session.cluster.shutdown()
                self.session.shutdown()
        except Exception as e:
            raise RuntimeError(f"Error al cerrar la conexión a Cassandra: {e}")
    
    def close_connection(self):
        """
        Cierra la conexión activa con Cassandra.
        """
        try:
            if self.session:
                self.session.cluster.shutdown()
                self.session.shutdown()
        except Exception as e:
            raise RuntimeError(f"Error al cerrar la conexión a Cassandra: {e}")