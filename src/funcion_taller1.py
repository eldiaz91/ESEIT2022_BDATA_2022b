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

def main():
    #Leer Archivo
    data=get_data("llamadas123_julio_2022.csv")
    #Hacerle tratamiento
    df_archivo = tratamiento(data)
    
    list_fechas= list()
    n_filas = df_archivo.shape[0]

    for i in range (0, n_filas):
        str_fecha = df_archivo['RECEPCION'][i]
        try:
            val_time=corrige_fecha(str_fecha)
            list_fechas.append(val_time)
        except Exception as e:
            print(i,e)
            list_fechas.append(str_fecha)
            continue

    df_archivo['RECEPCION_CORREGIDA'] = list_fechas

    #Guardar Resumen
    save_data(df_archivo, filename="llamadas123_julio_2022.csv")

def save_data(df, filename):
    root_dir=Path(".").resolve()
    out_name = 'limpio_' + filename
    out_path = os.path.join(root_dir, 'data','processed',out_name)
    df.to_csv(out_path)

def corrige_fecha(str_fecha):
    date_fecha=parse(str_fecha, dayfirst =False)  
    date_fecha=pd.to_datetime(parse(str_fecha, dayfirst =False) )

    return date_fecha

def tratamiento(data):
    #Borrando duplicados
    df_data = data.drop_duplicates()
    #Reemplace en la columna unidad los nulos por SIN_DATO (columna NaN)
    df_data['UNIDAD'].fillna('SIN DATO').value_counts(dropna=False)
    #Reemplazar SIN_DATO por un nulo de tipo numerico
    df_data['EDAD'].replace({'SIN_DATO' : np.nan}).value_counts

    return df_data


def get_data (filename):
    data_dir = 'raw'
    root_dir=Path(".").resolve()
    file_path=os.path.join(root_dir,"data",data_dir,filename)
    data = pd.read_csv(file_path, encoding='latin-1', sep =';')
    return data

if __name__ == '__main__':
    main()