FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# Ejecutar el script al iniciar el contenedor, usando python -u para ver los logs al instante
CMD ["python", "-u", "producer.py"]