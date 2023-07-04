"""
Creado por : Yohan Alfonso Hernandez
Fecha: 04-07-2023
Tema: lectura de archivos Json desde cloud storage
"""


# --Inicializar Gcloud
# gcloud init

# -- Autentuicarse en el projecto correspondiente
# gcloud project list

# -- Authenticate credentials for Google Cloud
# gcloud auth application-default login


""" este es el schema del archivo json a consultar en cloud storage
{
    "dag_name": "DAG_prueba",
    "project_id": "project-prod-yah",
    "staging": {
        "dataset_name": "stagin",
        "table_name": "tabla_prueba"
    },
    "raw": {
        "delta_bucket": "datalake_prod",
        "consolidate_bucket": "datalake_cons_prod",
        "dataset_name": "RAW",
        "table_name": "tabla_prueba_raw"
    }
}    
"""

# Imports the Google Cloud client library
import json
from google.cloud import storage


def parametros(json_file):
    
# Sacar los valores por objeto y clave segun se requieran
    project_id = json_file['project_id']

    stg_dataset_name = json_file['staging']['dataset_name']
    stg_table_name = json_file['staging']['table_name']

    raw_dataset_name = json_file['raw']['dataset_name']
    raw_table_name = json_file['raw']['table_name']
    
    print("el proyecto {p} tiene dos datasets:\n -- {stg} \n -- {raw} \n \
    a su ves tiene dos tablas correspondientemente:\n -- {stg_tabla} \n --{raw_tabla}"\
    .format(p=project_id, stg=stg_dataset_name, raw=raw_dataset_name, stg_tabla=stg_table_name, raw_tabla=raw_table_name ))


# Inicio de cliente en cloudstorage
storage_client = storage.Client()

bucket_name  = "bucket-pipeline-yah"
bucket = storage_client.get_bucket(bucket_name)
blobs = storage_client.list_blobs(bucket_name)

for blob in blobs:
    #print(blob.name)
    file = blob.name.split('/')[-1]
    if file != '':  # si el archivo no viene en blanca
        blob_file = bucket.get_blob(blob.name) #ruta completa de bicket con subcarpeta y archivos
       # print(blob_file)
        json_file = json.loads(blob_file.download_as_string()) #descarga el archivo como str y lo crea como json
       # print("este es el schema json file", json_file)
       
       
parametros(json_file)
