FROM apache/spark:3.5.0
USER root
# Installer Python et pip si nécessaire
RUN apt-get update && apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*
# Installer les dépendances Python
RUN pip3 install kafka-python pandas numpy
# Créer le répertoire de travail
WORKDIR /app
# Copier l'application
COPY streaming_app.py /app/
# Changer l'utilisateur pour spark (par sécurité)
USER spark