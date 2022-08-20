# importacion de librerías
from funciones import *

# main
# variables de configuracion
dataset_dir = 'datasets'
# credenciales de la base de datos
host, dbname, user, password = credenciales()
# configuracion del ambiente y creacion del dataframe
csv_files = csv_files()
if len(csv_files) == 0:  # ya existe la carpeta
    data_path = os.getcwd() + '/' + dataset_dir
    csv_files = recuperar_csv_files(data_path)

configure_dataset_directory(csv_files, dataset_dir)
df = create_df(dataset_dir, csv_files)

for k in csv_files:
    dataframe = df[k]  # llamar al dataframe
    tbl_name = clean_tbl_name(k)  # limpiar el nombre del archivo sera el nombre de la tabla
    col_str, dataframe.columns = clean_colname(dataframe)  # limpiar el nombre de las columnas, sera el atributo
    # ingresar los datos a la base de datos
    upload_to_db(host, dbname, user, password, tbl_name, col_str, k, dataframe, dataframe.columns)
print('Todas las tablas de datos han sido importadas correctamente')