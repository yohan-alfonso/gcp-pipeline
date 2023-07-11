"""
Autor: Yohan Alfonso Hernandez
Fecha: 11-07-2023
Tema: lectura de archivo CSV file, se realiza transformación y se envia a una tabla en bigquery
"""

# Importe de librerias
import re
import logging
import argparse
import apache_beam as beam
from google.cloud import bigquery
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions


# Funciones de transformación 
class split_and_lowercase(beam.DoFn): # transforma los caracteres a minusculas
    def process(self, df):
        words = df.split(',')
        lowercased_words = [word.lower() for word in words]
        return [lowercased_words]
    
class filter_mean_salary(beam.DoFn): # Realiza filtro en la columna 2 
    def process(self, column):
        column_value = column[3]
        if column_value != '0.0':
            return [column]

class SelectFields(beam.DoFn): # hace la unión de cada fila 
    def process(self, element):
        yield ','.join (element[1:5])
   
class FillEmptyWithNA(beam.DoFn): # Reemplaza valores en blaco con NA
    def process(self, element):
        element_=element.split(',')
        modified_element = []
        for value in element_:
            if value.strip(',') == '':
                modified_element.append("NA")
            else:
                modified_element.append(value)
        yield modified_element
        
        
class DataIngestion: #Transforma el archivo en formato de diccionario para facilitar la carga a bigquery
    def parse_method(self, string_input):
        values = re.split(",", re.sub('\r\n', '', re.sub('"', '', string_input)))
        row = dict(zip(('initial_sal', 'final_sal', 'avg_sal', 'job_type'),values))
        return row

def run(argv=None): # Inicio de funciones de pipeline en apache beam
    data_ingestion = DataIngestion()
    parser = argparse.ArgumentParser()
    args, beam_args = parser.parse_known_args()
    
    beam_options = PipelineOptions( # Parametros del pipeline
    beam_args,
    runner='DataflowRunner',
    project='gcp-pipeline-391818',
    job_name='dataflow-job-name-yah8',
    temp_location='gs://storage-beam-temp/temp',
    region='southamerica-west1')
    
    
    pipeline=  beam.Pipeline(options=beam_options)
    
    lines = pipeline | "ReadFiles" >> ReadFromText('gs://storage-beam-yah/salarios.csv') # lectura de archivo
        
    transformed_data = ( # Inicio de pipeline de transformación
    lines 
    | "SplitAndLowercase" >> beam.ParDo(split_and_lowercase())
    | "filter column salary" >> beam.ParDo(filter_mean_salary()) 
    | "segregate data" >>  beam.ParDo(SelectFields())
    | "Fill empty spaces with NA" >> beam.ParDo(FillEmptyWithNA())
    | "Join elements" >> beam.Map(lambda elements: ','.join(str(e) for e in elements))
    )
    
      
    wirte_to_bq = ( # inicio de pcollection para escritura a bigquery
    transformed_data
        
        | 'String To BigQuery Row' >>  beam.Map(lambda s: data_ingestion.parse_method(s)) 
        | 'Write to BigQuery' >> beam.io.Write( beam.io.BigQuerySink(
        table  = 'gcp-pipeline-391818.RRHH.salarios',
        schema ='initial_sal:FLOAT,final_sal:FLOAT,avg_sal:FLOAT,job_type:STRING',
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))      
    
    pipeline.run().wait_until_finish()  
    
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
