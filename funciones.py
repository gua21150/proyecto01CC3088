import os
import pandas as pd
import psycopg2


# encontrar los archivos csv dentro de la carpeta y preparar la data
def csv_files():
    csv_name_files = []
    for file in os.listdir(os.getcwd()):  # esto es una lista de archivos
        if file.endswith('.csv'):
            csv_name_files.append(file)
    return csv_name_files


def recuperar_csv_files(path):
    csv_name_files = []
    for file in os.listdir(path):  # esto es una lista de archivos
        if file.endswith('.csv'):
            csv_name_files.append(file)
    return csv_name_files


# crea y mueve los archivos csv a un solo directorio """
def configure_dataset_directory(csv_name_files, dataset_dir, op_sys):
    # creacion de la carpeta
    try:
        mkdir = 'mkdir {0}'.format(dataset_dir)
        os.system(mkdir)
        print(mkdir)
    except:
        pass
    # mover los cvs files a un nuevo directorio
    mv_file = ""
    for csv in csv_name_files:
        if op_sys == 1:
            mv_file = "move {0} {1}".format(csv, dataset_dir)  # para windows es move
        elif op_sys == 2:
            mv_file = "mv {0} {1}".format(csv, dataset_dir)  # para ios es mv
        os.system(mv_file)


# creacion de pandas df de un archivo csv"""
def create_df(dataset_directory, files):
    data_path = os.getcwd() + '/' + dataset_directory + '/'  # obtiene el path actual y lo que esta en el dataset
    df = {}  # diccionario
    for file in files:
        try:
            df[file] = pd.read_csv(data_path + file)
        except UnicodeDecodeError:
            df[file] = pd.read_csv(data_path + file, encoding="ISO-8859-1")
    return df  # se retorna el directorio


# Limpieza de los nombres de tablas y columnas """
# diccionario con los tipos de datos para las tablas en la base de datos
replacements = {
    'object': 'varchar',
    'float64': 'float',
    'int64': 'int',
    'datetime64': 'timestamp',
    'timedelta64[ns]': 'varchar'
}


# Limpieza del nombre del archivo """
def clean_tbl_name(filename):
    clean_table_name = filename.lower().replace(" ", "_").replace("?", "") \
        .replace("-", "_").replace(r"/", "_").replace("\\", "_").replace("%", "") \
        .replace(")", "").replace(r"(", "").replace("$", "")
    # quitar la extension .csv
    return '{0}'.format(clean_table_name.split('.')[0])  # nombre de la tabla


# Cambiar los nombres de las columnas a un formato mas limpio"""
def clean_colname(data_frame):
    data_frame.columns = [x.lower().replace(" ", "_").replace("?", "").replace("-", "_") \
                          .replace(r"/", "_").replace("\\", "_")\
                          .replace("%", "").replace(")", "").replace(r"(", "").replace("$", "") \
                          for x in data_frame.columns]
    # table schema
    col_str = ", ".join(
        "{} {}".format(n, d) for (n, d) in zip(data_frame.columns, data_frame.dtypes.replace(replacements)))
    return col_str, data_frame.columns


# Conecta a la base de datos e ingresa los datos
def upload_to_db(host, dbname, user, password, tab_name, col_names, file, data_frame, data_frame_columns):
    # conexion a la base de datos
    conn_str = "host=%s dbname=%s user=%s password=%s" % (host, dbname, user, password)
    try:
        conn = psycopg2.connect(conn_str)
        cursor = conn.cursor()
        print("Sesion a la base de datos ha sido exitosa")
        """ creacion de tablas en base de datos """
        # borrar tablas en caso que existan
        cursor.execute("DROP TABLE IF EXISTS %s" % tab_name)
        # crear las tablas
        cursor.execute("CREATE TABLE %s (%s)" % (tab_name, col_names))
        print("{0} creada exitosamente".format(tab_name))

        # insertar los valores a las tablas
        data_frame.to_csv(file, header=data_frame_columns, index=False, encoding='utf-8')
        my_file = open(file)
        print('Archivo en memoria ha sido abierto')
        sql_statement = """
               COPY %s FROM STDIN WITH 
                   CSV
                   HEADER
                   DELIMITER AS ','
               """
        cursor.copy_expert(sql=sql_statement % tab_name, file=my_file)
        print('Archivo copiado a la base de datos')
        cursor.execute("GRANT SELECT ON TABLE %s TO PUBLIC" % tab_name)
        conn.commit()
        conn.close()
        print('La tabla {0} ha sido importada a la base de datos'.format(tab_name))
        print('---------------------------')
    except psycopg2.OperationalError:
        print("Alguna credencial no ha sido ingresada correctamente")


def credenciales():
    host = str(input("Ingrese el host de su base de datos: "))
    db = str(input("Ingrese el nombre de la base de datos: "))
    user = str(input("Ingrese el usuario de pgadmin de su base de datos: "))
    passw = str(input("Ingrese la contraseña de su usuario de base de datos: "))
    return host, db, user, passw


def sys():
    try:
        print("Su equipo es sistema operativo: \n1. Windows \n2. iOS")
        return int(input("Ingrese el tipo de sistema de su equipo "))
    except:
        print("Ingrese un numero entero valido.\nVuelva a iniciar el programa")
