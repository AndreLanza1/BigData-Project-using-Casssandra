# Usiamo un'immagine Python ufficiale e leggera
FROM python:3.11-slim

# Creiamo una cartella di lavoro
WORKDIR /app

# Copiamo i requisiti e installiamo le librerie
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamo i nostri fantastici script
COPY . .

# Lasciamo il terminale aperto per poter lanciare i comandi
CMD ["bash"]