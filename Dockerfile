# Usa una imagen de Python como base
FROM python:3.9.0

# Instala las dependencias especificadas en requirements.txt
RUN pip install --no-cache-dir -r requirements.txt


# Comando por defecto para ejecutar tu aplicación
#CMD ["python", "app.py"]
