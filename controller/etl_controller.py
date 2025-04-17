import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
"""Este módulo coordina las funciones del ETL."""
from utils.mongo_handler import DatabaseHandlerMongo
from utils.cassandra_handler import DatabaseHandlerCassandra

def run_etl_process(action: str, param: str) -> None:
    """
    Coordina las acciones principales del ETL.

    Args:
        action (str): Acción a ejecutar ('init', 'cargar', etc.).
        param (str): Parámetro adicional según la acción (ruta de archivo, consulta, etc.).
    """
    if action == "init":
        # Validar conexiones
        mongo_handler = DatabaseHandlerMongo()
        mongo_handler.validate_connection()
        mongo_handler.collection.create_index([("TextoExtraido", "text")])
        cassandra_handler = DatabaseHandlerCassandra()
        cassandra_handler.close_connection()
        print("Conexiones validadas correctamente.")

    elif action == "extraer":
        
        mongo_handler = DatabaseHandlerMongo()
        cassandra_handler = DatabaseHandlerCassandra()
        mongo_handler.collection.create_index([("TextoExtraido", "text")])
        try:
            path_cleaned = param.replace("resources\\Pdf\\", "").replace("resources/Pdf/", "").rstrip(".pdf")
            print(f"Extrayendo el texto del archivo='{param}'")
            print(f"Buscando en Cassandra con id_doc='{path_cleaned}'")
            data_from_cassandra = cassandra_handler.fetch_from_cassandra(path_cleaned)
            if not data_from_cassandra:
                print("No se encontraron datos en Cassandra para el identificador.")
                raise RuntimeError(f"No se encontraron datos en Cassandra para id_doc='{path_cleaned}'")
            document = mongo_handler.add_pdf(param, data_from_cassandra)

        except Exception as e:
            print(f"Error al procesar el archivo {param}: {e}")

        finally:
            mongo_handler.close_connection()
            cassandra_handler.close_connection()

    elif action == "clear":
        # Limpiar datos en MongoDB
        mongo_handler = DatabaseHandlerMongo()
        mongo_handler.collection.delete_many({})
        mongo_handler.close_connection()
        print("Todos los datos eliminados en MongoDB.")

def buscar_en_mongo(palabra: str) -> None:
    """
    Realiza una búsqueda en MongoDB por palabra clave.

    Args:
        palabra (str): Palabra clave a buscar.
    """
    mongo_handler = DatabaseHandlerMongo()

    try:
        indices = mongo_handler.collection.index_information()
        if "TextoExtraido_text" not in indices:
            mongo_handler.collection.create_index([("TextoExtraido", "text")])

        results = mongo_handler.buscar_por_palabra_clave(palabra)

        if results:
            print(f"Se encontraron {len(results)} documentos con la palabra clave '{palabra}':")
            mongo_handler.print_result(results) 
        else:
            print(f"No se encontraron documentos que contengan la palabra clave '{palabra}'.")

    except Exception as e:
        print(f"Error al buscar en MongoDB: {e}")

    finally:
        mongo_handler.close_connection()
    