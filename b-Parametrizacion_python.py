"""
Creado por : Yohan Alfonso Hernandez
Fecha: 04-07-2023
Tema: lectura de archivos Json desde python con la siguiente estructura:


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

#-- Lee el archivo Json dag.jeson

import os 
import json


lista =  os.listdir()
print(lista)

dag_file =  open(lista[1])  # selecciona el archivo json por su indice

data = json.load(dag_file)

print (data)

for i in data:
    print(i)

for r in data['raw']:
    print("estos son datos raw2", r)


for s in data['staging']:
    print("estos son datos staging", s)
    
    
print ( data['raw']['delta_bucket'])
 

