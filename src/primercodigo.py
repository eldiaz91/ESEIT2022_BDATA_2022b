from tabnanny import verbose
import numpy as np
import argparse as ap
import pandas as pd

def main():

    parser=ap.ArgumentParser()
    parser.add_argument("--verbose",type=int, default=1, help="Para imprimir informacion en pantalla")
    
    args=parser.parse_args()
    
    lista_valores = [1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39]
    calcular_valores(lista_valores,verbose=args.verbose)

def calcular_min_max(lista_numeros, verbose):
    """Calcula el Minimo y Maximo de una lista de numeros

    Args:
        lista_numeros (list): lista con valores numericos
        verbose (bool, optional): Decidir si imprimir el mensaje en pantalla  Defaults to True.

    Returns:
        tuple: con el minimo y maximo
    """
    
    min_value=min(lista_numeros)
    max_value=max(lista_numeros)

    if verbose==1:
        print("el valor minimo es: ", min_value)
        print("el valor maximo es: ", max_value)
    else:
        pass

    return min_value, max_value


def calcular_valores_centrales(lista_numeros, verbose):
    """_summary_

    Args:
        lista_numeros (list): lista con valores numericos
        verbose (bool, optional): Decidir si imprimir el mensaje en pantalla  Defaults to True.

    Returns:
        tuple: (media,dev_std)
    """

    media=np.mean(lista_numeros)
    dev_std=np.std(lista_numeros)

    if verbose==1:
        print("La media es: ", media)
        print("La desviacion estandar es: ", dev_std)
    else:
        pass


    return media, dev_std

def calcular_valores(lista_numeros, verbose):

    suma= np.sum(lista_numeros)
    print("La suma es",suma)
    # suma = calcular_suma(lista_numeros)
    min_val, max_val = calcular_min_max(lista_numeros,verbose)
    media, dev_std = calcular_valores_centrales(lista_numeros,verbose)

    return suma, min_val, max_val, media, dev_std

    
if __name__ == '__main__':
    main()