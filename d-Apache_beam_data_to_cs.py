"""
Creado por: Yohan Alfonso Hernandez
Fecha: 11-07-2023
tema: Reliza al lectura de un archivo csv, se hacen transformaciones y finalmente
se convierte en diccionario con el doble proposito de llevarlo a Cloud Storage
"""
# Importe de librerias
import re
import logging
import argparse
import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

# Funciones de transformación

class split_and_lowercase(beam.DoFn): # Pasa todas los caracteres a miniscula
    def process(self, df):
        # Split the element into individual words
        words = df.split(',')
        # Convert each word to lowercase
        lowercased_words = [word.lower() for word in words]
        # Return the lowercase words as a list
        return [lowercased_words]
    
class filter_mean_salary(beam.DoFn): # Realiza filtrado en la columna 2
    def process(self, column):
        column_value = column[3]
        if column_value != '0.0':
            return [column]
       
class SelectFields(beam.DoFn): # realiza el join de las columnas 2 a la 4
    def process(self, element):
        yield ','.join (element[1:5])
        
        
        
class FillEmptyWithNA(beam.DoFn): # Rellena con NA las celdas en blanco
    def process(self, element):
        element_=element.split(',')
        modified_element = []
        for value in element_:
            if value.strip(',') == '':
                modified_element.append("NA")
            else:
                modified_element.append(value)
        yield modified_element

class DataIngestion:
     def parse_method(self, string_input): # tansforma la salida en forma de diccionario (esto para posteriosmente llevarlo a BQ)

        # Strip out carriage return, newline and quote characters.
        values = re.split(",", re.sub('\r\n', '', re.sub('"', '',string_input)))
        row = dict(zip(('initial_sal', 'final_sal', 'avg_sal', 'job_type'), values))
        return row    
        
def run(inputs_pattern, output_prefix): # Función de inicio de apache Beam
    
    data_ingestion = DataIngestion() # llama la funcuión de cambio de archivo a formato diccionario
    
    parser = argparse.ArgumentParser()
    args, beam_args = parser.parse_known_args()
    
    beam_options = PipelineOptions(  #Parametros con se ejecuta el pipeline
    beam_args,
    runner='DataflowRunner',
    project='gcp-pipeline-391818',
    job_name='dataflow-job-name-yah',
    temp_location='gs://storage-beam-temp/temp',
    region='us-central1')

    with beam.Pipeline() as pipeline: # Inicio del pipeline en pache beam
        
        lines = pipeline | "ReadFiles" >> ReadFromText(inputs_pattern)

        transformed_data = (
        lines 

        | "SplitAndLowercase" >> beam.ParDo(split_and_lowercase())
        | "filter column" >> beam.ParDo(filter_mean_salary()) 
        | "selected_fields" >> beam.ParDo(SelectFields())
        | "Fill empty spaces with NA" >> beam.ParDo(FillEmptyWithNA())
        | "Join elements" >> beam.Map(lambda elements: ','.join(str(e) for e in elements))
        | 'String To BigQuery Row' >>  beam.Map(lambda s: data_ingestion.parse_method(s)) 
        )
        
        transformed_data | beam.Map(print) # print de salida de datos
        #transformed_data |"WriteToFile" >> WriteToText(output_prefix, file_name_suffix='.csv') # auitar comentario para enviar a cloud Storage
        
if __name__ == '__main__':
    inputs_pattern = 'gs://storage-beam-yah/salarios.csv'
    #output_prefix= 'gs://storage-beam-yah/output_salarios' # Quitar comentario para enviar a Cloud Storage
    output_prefix= 'output_salarios' #Comentariar si no que quiere salida en panatalla
    run(inputs_pattern,output_prefix) # Envia parametros de entrada y salida en finción run
      
