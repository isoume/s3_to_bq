from google.cloud import bigquery
from google.api_core.exceptions import NotFound

def gcs_to_bq(event, context):
    bucket = event['bucket']
    file_name = event['name']
    uri = f"gs://{bucket}/{file_name}"

    dataset_id = "demo"
    table_id = "mytable"
    full_table_id = f"{dataset_id}.{table_id}"

    print(f"Fichier détecté : {uri}")

    client = bigquery.Client()

    # Vérifier si la table existe
    try:
        client.get_table(full_table_id)
        print(f"La table {full_table_id} existe déjà.")
    except NotFound:
        print(f"La table {full_table_id} n'existe pas. Création...")

        # Exemple de schéma minimal - tu peux l’ajuster
        schema = [
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("age", "INTEGER"),
            bigquery.SchemaField("city", "STRING"),
        ]

        table = bigquery.Table(full_table_id, schema=schema)
        client.create_table(table)
        print(f"Table {full_table_id} créée !")

    # Charger le fichier CSV dans la table
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,          # détecte d'autres colonnes si présentes
        skip_leading_rows=1
    )

    load_job = client.load_table_from_uri(
        uri,
        full_table_id,
        job_config=job_config
    )

    print("Chargement en cours…")
    load_job.result()  # attendre la fin du job
    print("Chargement terminé avec succès !")
