# Base Python
FROM python:3.11-slim

# Définir le répertoire de travail
WORKDIR /app

# Copier tous les fichiers de l’API dans le container
COPY . /app

# Installer les dépendances
RUN pip install --no-cache-dir flask pymongo

# Exposer le port Flask
EXPOSE 5000

# Commande pour lancer l’API
CMD ["python", "app.py"]
