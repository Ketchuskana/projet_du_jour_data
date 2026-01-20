# Base image Python
FROM python:3.11-slim

# Définir le répertoire de travail
WORKDIR /app

# Copier les fichiers du dashboard
COPY . /app

# Installer les dépendances
RUN pip install --no-cache-dir streamlit requests

# Exposer le port Streamlit
EXPOSE 8501

# Commande pour lancer l'app
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
