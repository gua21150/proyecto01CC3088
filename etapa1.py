# importacion de librerías
import os
import numpy as np
import pandas as pd
import psycopg2
from configparser import ConfigParser


csv_files = []
""" encontrar los archivos csv dentro de la carpeta y preparar la data """
for file in os.listdir(os.getcwd()):  # esto es una lista de archivos
    if file.endswith('.csv'):
        csv_files.append(file)

# realizar un nuevo directorio
dataset_dir = 'datasets'
try: 
    mkdir = 'mkdir {0}'.format(dataset_dir)
    os.system(mkdir)
    print(mkdir)
except:
    pass

# mover los cvs files a un nuevo directorio
for csv in csv_files:
    mv_file = 'move {0} {1}'.format(csv, dataset_dir)  # para windows es move para ios es mv
    os.system(mv_file)
    print(mv_file)

"""creacion de pandas df de un archivo csv"""
data_path = os.getcwd()+'/'+dataset_dir+'/'
df = {}
for file in csv_files:
    try:
        df[file] = pd.read_csv(data_path+file)
    except UnicodeDecodeError:
        df[file] = pd.read_csv(data_path+file, encoding = "ISO-8859-1")
    print(file)

