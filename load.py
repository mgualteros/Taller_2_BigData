from controller.etl_controller import run_etl_process
import os

def procesar_archivos():
    """
    Procesa y carga múltiples archivos en MongoDB.
    """
    try:
        directorio = "resources/Pdf/"
        archivos = [os.path.join(directorio, archivo) for archivo in os.listdir(directorio) if archivo.endswith(".pdf")]

        print(f"Se encontraron {len(archivos)} archivos para procesar.")

        for archivo in archivos:
            print(f"Procesando archivo...")
            try:
                run_etl_process("extraer", archivo)  
                print(f"El texto de {archivo} ha sido extraído y guardado en mongo")
            except Exception as e:
                print(f"Error al procesar {archivo} en MongoDB: {e}")

        print("Todos los archivos fueron procesados exitosamente en MongoDB.")
    
    except Exception as e:
        print(f"Error durante el procesamiento de archivos: {e}")