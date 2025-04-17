"""Este módulo proporciona el CLI para el taller2 de ETL."""
import typer
from controller.etl_controller import run_etl_process, buscar_en_mongo
from utils.mongo_handler import DatabaseHandlerMongo
from utils.cassandra_handler import DatabaseHandlerCassandra
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from etl.config import BUNDLE_PATH, TOKEN_PATH, CASSANDRA_KEYSPACE
import json

app = typer.Typer()

@app.command(name="init")
def init() -> None:
    """
    Inicializa las conexiones con las bases de datos, limpia los datos existentes y ejecuta el proceso de carga.
    """
    try:
        typer.secho(f"Iniciando conexiones y limpiando bases de datos...")
        # Conexión a MongoDB y limpieza
        mongo_handler = DatabaseHandlerMongo()
        mongo_handler.collection.delete_many({})
        typer.secho(f"Colección MongoDB vaciada.", fg=typer.colors.BLUE)

        # Conexión y limpieza de Cassandra
        cloud_config = {"secure_connect_bundle": BUNDLE_PATH}
        with open(TOKEN_PATH) as f:
                secrets = json.load(f)
        
        auth_provider = PlainTextAuthProvider(
                secrets["clientId"], secrets["secret"]
            )
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        session = cluster.connect()
        
        query = """
        CREATE TABLE IF NOT EXISTS taller2.entidad_por_doc (
            id_doc TEXT,
            Entidad TEXT,
            Resumen TEXT,
            Fecha DATE,
            PRIMARY KEY ((id_doc, Entidad), Fecha)
        )
        ;"""
        session.execute(query)

        typer.secho(f"Tabla 'entidad_por_doc' creada en Cassandra.", fg=typer.colors.GREEN)
        typer.secho(f"Cargando archivos en MongoDB desde 'load.py'...")
        
        import load
        load.procesar_archivos() 

        typer.secho(f"Cargando datos en Cassandra desde 'Query_resumen.xlsx'...")

        df = pd.read_excel('resources\Query_resumen.xlsx')
        for index, row in df.iterrows():
            id_doc = row.iloc[0]  
            resumen = row.iloc[1] 
            fecha = row.iloc[3] 
            entidades = row.iloc[2].split(',') 

            for entidad in entidades:
                insert_query = """
                INSERT INTO taller2.entidad_por_doc (id_doc, entidad, resumen, fecha)
                VALUES (%s, %s, %s, %s)
                """
                session.execute(insert_query, (id_doc.strip(), entidad.strip(), resumen.strip(), fecha))
        
        typer.secho(f"Datos cargados en Cassandra exitosamente!", fg=typer.colors.GREEN)
        typer.secho(f"Proceso de inicialización completado!", fg=typer.colors.GREEN)

    except Exception as e:
        typer.secho(f"Error durante la inicialización: {e}", fg=typer.colors.RED)

@app.command(name="extraer")
def cargar_archivo(path: str) -> None:
    """
    Carga un archivo a le extrae el texto y lo carga en mongo.

    Args:
        path (str): Ruta del archivo.
    """
    try:
        # Procesar y cargar en MongoDB
        run_etl_process("extraer", path)
        typer.secho(f"Archivo {path} procesado exitosamente!", fg=typer.colors.GREEN)

    except Exception as e:
        typer.secho(f"Error al cargar archivo: {str(e)}", fg=typer.colors.RED)
        raise typer.Exit(1)

@app.command(name="buscar")
def buscar_palabra_clave(palabra: str) -> None:
    """
    Realiza una búsqueda en MongoDB por palabra clave.
    
    Args:
        palabra (str): Palabra clave a buscar.
    """
    try:
        buscar_en_mongo(palabra)
    except Exception as e:
        typer.secho(f"Error al buscar en MongoDB: {str(e)}", fg=typer.colors.RED)
        raise typer.Exit(1)

@app.command(name="limpiar")
def limpiar_mongo(force: bool = typer.Option(..., prompt="¿Quiere eliminar todos los datos de MongoDB?", help="Confirmar eliminación.")) -> None:
    """
    Elimina todos los datos de MongoDB.
    
    Args:
        force (bool): Confirmación para eliminar los datos.
    """
    if force:
        try:
            run_etl_process("clear", None)
            typer.secho("Todos los datos eliminados exitosamente en MongoDB.", fg=typer.colors.GREEN)
        except Exception as e:
            typer.secho(f"Error al limpiar MongoDB: {str(e)}", fg=typer.colors.RED)
            raise typer.Exit(1)
    else:
        typer.secho("Operación cancelada.", fg=typer.colors.YELLOW)

@app.command(name="status")
def status() -> None:
    """
    Verifica el estado de las conexiones a MongoDB y Cassandra.
    """
    try:
        # Validación MongoDB
        mongo_handler = DatabaseHandlerMongo()
        mongo_handler.validate_connection()
        typer.secho("MongoDB está operativo.", fg=typer.colors.GREEN)

        # Validación Cassandra
        cassandra_handler = DatabaseHandlerCassandra()
        cassandra_handler.close_connection()  # Solo validamos y cerramos
        typer.secho("Cassandra está operativo.", fg=typer.colors.GREEN)
    except Exception as e:
        typer.secho(f"Error al verificar el estado: {str(e)}", fg=typer.colors.RED)
        raise typer.Exit(1)

if __name__ == "__main__":
    app()