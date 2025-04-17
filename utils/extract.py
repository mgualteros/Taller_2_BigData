import fitz 
from PIL import Image
import io
import pytesseract

def extraer_texto_imagenes_pdf(ruta_pdf):

    doc = fitz.open(ruta_pdf)
    texto_total = ""

    for i, pagina in enumerate(doc):
        for img_index, img in enumerate(pagina.get_images(full=True)):
            xref = img[0]
            base_imagen = doc.extract_image(xref)
            imagen_bytes = base_imagen["image"]

            # Convertir los bytes en imagen PIL
            imagen = Image.open(io.BytesIO(imagen_bytes))

            # Extraer texto
            extracted_text = pytesseract.image_to_string(imagen, lang='eng')
            texto_total += f"\n\n--- Página {i+1}, Imagen {img_index+1} ---\n"
            texto_total += extracted_text

    doc.close()
    return texto_total