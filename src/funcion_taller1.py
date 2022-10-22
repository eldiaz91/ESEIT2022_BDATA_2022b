#Resumen del py : 
#1. Leer datos: Leer archivo .csv (origen de datos)
#2. Extraer el resumen solicitado, para este caso es la cantidad de valores unicos de todas las columnas
#3. Guardar el resumen en formato csv. 

from pkgutil import get_data

import numpy as np
import pandas as pd
import os 
from pathlib import Path 
from dateutil.parser import parse
bucket='gs://hd_bucket_llamadas123/'
import logging


def main():
    #Configuracion logs:
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    logging.captureWarnings(True)
    logging.basicConfig(level=logging.INFO, format=log_fmt, filename='/home/jupyter/ESEIT2022_BDATA_2022b/data/logs/etl_llamadas.log')
    
    #Leer Archivo
    data=get_data("llamadas123_agosto_2022.csv")
    #Hacerle tratamiento
    df_archivo = tratamiento(data)
    #Guardar Resumen
    save_data(df_archivo, filename="llamadas123_agosto_2022.csv")

    logging.info ('DONE!!!')
    
def save_data(df, filename):
    #root_dir=Path(".").resolve()
    out_name = 'limpio_' + filename
    out_path = os.path.join(bucket, 'data','processed',out_name)
    df.to_csv(out_path)
    
    #Guardar Tabla en BigQuery
    print("Cargando archivo :",filename)
    df.to_gbq(destination_table='espbigdata.llamadas_123', if_exists='append')

def convertir_fecha(str_fecha):
    date_fecha=parse(str_fecha, dayfirst =True)  
    return date_fecha

def corregir_fecha (data):
    list_fechas = list()
    n_filas = data.shape[0]
    for i in range(0, n_filas):
        str_fecha = data["RECEPCION"][i]
        try:
            val_datetime = convertir_fecha(str_fecha= str_fecha)
            list_fechas.append(val_datetime)
        except Exception as e:
            list_fechas.append(str_fecha)
        continue

    data["RECEPCION_CORREGIDA"] = list_fechas
    data["RECEPCION_CORREGIDA"] = pd.to_datetime(data["RECEPCION_CORREGIDA"], errors = "coerce")
    data["RECEPCION"] = data["RECEPCION_CORREGIDA"]
    data = data.drop(["RECEPCION_CORREGIDA"], axis=1)


def tratamiento(data):
    #Borrando duplicados
    df_data = data.drop_duplicates()
    #Reemplace en la columna unidad los nulos por SIN_DATO (columna NaN)
    df_data['UNIDAD'].fillna('SIN DATO').value_counts(dropna=False)
    #Reemplazar SIN_DATO por un nulo de tipo numerico
    df_data['EDAD'].replace({'SIN_DATO' : np.nan}).value_counts
    # Convertir columna en formato fecha
    col = "FECHA_INICIO_DESPLAZAMIENTO_MOVIL"
    data[col] = pd.to_datetime(data[col], errors = "coerce")
    #Reemplazar las localidades que tienen caracteres especiales
    data['LOCALIDAD']= data['LOCALIDAD'].replace(['Fontib¢n','Engativ ',"Ciudad Bol¡var","Usaqun","San Crist¢bal","Los M rtires","Antonio Nari¤o"],['Fontibon','Engativa',"Ciudad Bolivar","Usaquen","San Cristobal", "Los Martires","Antonio Nariño"])
    #corregir la fecha de recepcion
    corregir_fecha(data)
    return df_data


def get_data (filename):
    logger=logging.getLogger('get_data')
    #data_dir = 'raw'
    #root_dir=Path(".").resolve()
    #file_path=os.path.join(root_dir,"data",data_dir,filename)
    dir_data= os.path.join(bucket,"data","raw")
    logger.info(f'Reading File: {dir_data}')
    data = pd.read_csv(os.path.join(dir_data,filename), encoding='latin-1', sep =';')
    logger.info(f'La Tabla contiene: {data.shape[0]} filas y {data.shape[1]} columnas')
    return data

if __name__ == '__main__':
    main()
