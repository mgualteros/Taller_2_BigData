from databasemongo import DatabaseHandlerMongo
from etl.cassandra_handler import DatabaseHandlerCassandra

if __name__ == "__main__":
    mongo_handler = DatabaseHandlerMongo()
    cassandra_handler = DatabaseHandlerCassandra()

    path_pdf = "Pdf\\A-491-23.pdf"

    path_cleaned = path_pdf.replace("Pdf\\", "").rstrip(".pdf")
    data_from_cassandra = cassandra_handler.fetch_from_cassandra(path_cleaned)
    resultado = mongo_handler.add_pdf(path_pdf, data_from_cassandra)
    print("Documento guardado en MongoDB:")

from databasemongo import DatabaseHandlerMongo  # Manejador de MongoDB
from etl.cassandra_handler import DatabaseHandlerCassandra  # Manejador de Cassandra

def main():
    mongo_handler = DatabaseHandlerMongo()
    cassandra_handler = DatabaseHandlerCassandra()

    path_cleaned = "A-480-23"
    try:
        data_from_cassandra = cassandra_handler.fetch_from_cassandra(path_cleaned)
        print(f"Datos obtenidos de Cassandra: {data_from_cassandra}")

        # Agregar un documento a MongoDB
        path_pdf = "Pdf\\A-480-23.pdf"
        documento_insertado = mongo_handler.add_pdf(path_pdf, data_from_cassandra)
        print(f"Documento insertado en MongoDB: {documento_insertado}")

    except RuntimeError as e:
        print(f"Error durante el flujo de Cassandra/MongoDB: {e}")

    try:
        palabra_clave = "recurso"
        resultados = mongo_handler.buscar_por_palabra_clave(palabra_clave)
        print(f"Resultados de búsqueda por palabra clave '{palabra_clave}':")
        for resultado in resultados:
            print(resultado)
    except RuntimeError as e:
        print(f"Error durante la búsqueda en MongoDB: {e}")

if __name__ == "__main__":
    main()