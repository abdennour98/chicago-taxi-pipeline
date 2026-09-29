# ÉTAPE 1 - BRONZE : télécharger les courses de taxi (API Chicago) et les déposer en Parquet dans MinIO
import io
import os

import boto3
import pandas as pd
import requests

URL = "https://data.cityofchicago.org/resource/wrvz-psew.csv"
LIGNES_PAR_PAGE = 20000

# Les mois à télécharger (début inclus, fin exclue) : 1er trimestre 2023
MOIS = [
    ("2023-01-01", "2023-02-01"),
    ("2023-02-01", "2023-03-01"),
    ("2023-03-01", "2023-04-01"),
]

# Connexion à MinIO
s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["S3_ENDPOINT"],
    aws_access_key_id=os.environ["S3_ACCESS_KEY"],
    aws_secret_access_key=os.environ["S3_SECRET_KEY"],
    region_name="us-east-1",
)

for debut, fin in MOIS:
    # On ne demande à l'API que les courses de ce mois
    filtre = f"trip_start_timestamp >= '{debut}T00:00:00' AND trip_start_timestamp < '{fin}T00:00:00'"

    decalage = 0   # nombre de lignes déjà téléchargées
    numero = 0     # numéro du fichier Parquet du mois

    while True:
        # On demande une page de 50 000 lignes
        reponse = requests.get(URL, params={
            "$where": filtre,
            "$order": "trip_start_timestamp, trip_id",
            "$limit": LIGNES_PAR_PAGE,
            "$offset": decalage,
        }, timeout=120)
        reponse.raise_for_status()

        if reponse.text.strip() == "":
            break  # plus rien à télécharger

        # On lit la page (tout en texte : le brut n'est pas modifié) ...
        page = pd.read_csv(io.StringIO(reponse.text), dtype="string")

        # ... on l'enregistre en Parquet ...
        page.to_parquet("/tmp/page.parquet", index=False)

        # ... et on l'envoie dans le bucket bronze
        nom = f"taxi_trips/month={debut[:7]}/part-{numero:04d}.parquet"
        s3.upload_file("/tmp/page.parquet", "bronze", nom)

        numero = numero + 1
        decalage = decalage + LIGNES_PAR_PAGE

        # Moins de 50 000 lignes reçues = c'était la dernière page
        if len(page) < LIGNES_PAR_PAGE:
            break

    print("Mois", debut[:7], ":", numero, "fichier(s) Parquet envoyé(s) dans bronze")