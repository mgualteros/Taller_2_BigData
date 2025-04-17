import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from typing import Any, Dict, List
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from etl.config import MONGO_URI, MONGO_DB, MONGO_COLLECTION
from utils.extract import extraer_texto_imagenes_pdf
import json
from etl import MONGO_CON_ERROR, PDF_READ_ERROR, DB_WRITE_ERROR, SEARCH_ERROR, ERRORS

class DatabaseHandlerMongo:
    def __init__(self):
        """Inicializa la conexión a MongoDB."""
        try:
            self.client = MongoClient(MONGO_URI)
            self.db = self.client[MONGO_DB]
            self.collection = self.db[MONGO_COLLECTION]
        except PyMongoError as e:
            raise RuntimeError(f"{ERRORS[MONGO_CON_ERROR]}: {e}")

    def validate_connection(self):
        """Valida la conexión a MongoDB y lista bases de datos y colecciones."""
        print("Bases de datos disponibles en MongoDB:", self.client.list_database_names())
        print("Colecciones disponibles en la base de datos:", self.db.list_collection_names())


    def add_pdf(self, path: str, data_from_cassandra: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extrae texto de un PDF y guarda el resultado en MongoDB.

        Args:
            path (str): La ruta del archivo PDF.
            data_from_cassandra (Dict[str, Any]): Datos adicionales desde Cassandra.

        Returns:
            Dict[str, Any]: Documento final guardado en MongoDB.
        """
        if not path:
            raise ValueError("El 'path' no puede estar vacío.")

        # Extraer texto del PDF
        text = extraer_texto_imagenes_pdf(path)
        if not text or "No se pudo reconocer el texto" in text:
            raise RuntimeError(f"{ERRORS[PDF_READ_ERROR]}: {e}")

        # Preparar el documento final
        path_cleaned = path.replace("resources\\Pdf\\", "").replace("resources/Pdf/", "").rstrip(".pdf")
        fecha = data_from_cassandra.get("Fecha", None)
        fecha_formateada = fecha.strftime("%Y-%m-%d") if fecha else None
        document = {
            "Archivo": path_cleaned,
            "TextoExtraido": text,
            "Resumen": data_from_cassandra.get("Resumen", " "),
            "Entidad": data_from_cassandra.get("Entidad", []),
            "Fecha": fecha_formateada
        }

        # Insertar el documento en MongoDB
        try:
            result = self.collection.insert_one(document)
            document["_id"] = str(result.inserted_id)
            return document
        except PyMongoError as e:
            raise RuntimeError(f"{ERRORS[DB_WRITE_ERROR]}: {e}")
    
    def print_result(self, data):
        json_data = json.dumps(data, indent=4, sort_keys=True)
        print(json_data)
    
    def construir_consulta(self, palabra: str):
        """
        Construye la consulta y la proyección para la búsqueda en MongoDB.

        Args:
            palabra (str): Palabra clave para la búsqueda.

        Returns:
            tuple: Consulta y proyección.
        """
        query = {"$text": {"$search": f"\"{palabra}\""}}  # Comillas dobles para buscar frase exacta
        projection = {
            "Archivo": True,
            "TextoExtraido": True,
            "Resumen": True,
            "Entidad": True,
            "Fecha": True,
            "_id": False  # Excluir _id
        }
        return query, projection
    
    def buscar_por_palabra_clave(self, palabra: str) -> List[Dict[str, Any]]:
        """
        Busca documentos que contengan la palabra clave en el campo 'TextoExtraido'.

        Args:
            palabra (str): La palabra clave a buscar.

        Returns:
            List[Dict[str, Any]]: Lista de documentos que contienen la palabra clave.
        """
        try:
            query, projection = self.construir_consulta(palabra)
            resultados = self.collection.find(query, projection)
            return list(resultados)
        
        except PyMongoError as e:
            raise RuntimeError(f"Error al realizar la búsqueda: {e}")
            
    def close_connection(self):
        """Cierra la conexión activa con MongoDB."""
        try:
            self.client.close()
        except Exception as e:
            raise RuntimeError(f"Error al cerrar la conexión a MongoDB: {e}")