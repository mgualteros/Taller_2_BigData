"""Top-level package for ETL Project for Cassandra and MongoDB."""
__app_name__ = "etl_taller2"
__version__ = "0.1.0"
(
    SUCCESS,
    CASSANDRA_CON_ERROR,
    MONGO_CON_ERROR,
    PDF_READ_ERROR,
    FILE_FORMAT_ERROR,
    SEARCH_ERROR,
    DIR_ERROR,
    FILE_ERROR,
    DB_WRITE_ERROR,

) = range(9)

ERRORS = {
    CASSANDRA_CON_ERROR: "Error al conectar con Cassandra.",
    MONGO_CON_ERROR: "Error al conectar con MongoDB.",
    PDF_READ_ERROR: "Error al leer el archivo PDF.",
    FILE_FORMAT_ERROR: "Formato de archivo no válido.",
    SEARCH_ERROR: "Error al realizar la búsqueda.",
    DIR_ERROR: "Error al crear el directorio de configuración.",
    FILE_ERROR: "Error al crear el archivo de configuración.",
    DB_WRITE_ERROR: "Error al escribir en la base de datos.",
}