import os
from pathlib import Path
import pandas as pd
import shutil

path_dir=r'Z:\1. Coordinadores\Asignaciones\Informes Lead\Intentos maquina'

def read_file():
        
    list_file = os.listdir(path_dir)
    if not list_file:
        print('No hay archivos de lectura.')
        return None
    else:
        files = []
            
        for file in list_file:
            name_file = os.path.join(path_dir, file)
            df = pd.read_csv(name_file)
            files.append(df)
        
        df = pd.concat(files)
        print("Lectura de archivios finalizado.\n")
        
        
        return df


def eliminate_file():
    list_file_new_asignation = os.listdir(path_dir)
    for name in list_file_new_asignation:
        name_file_source = os.path.join(path_dir, name)
        os.remove(name_file_source)

        
    print(f'Se eliminaron {len(list_file_new_asignation)} archivos de la carpeta del repositorio.')
    
    
def read_data_db():
    text = """
        SELECT      *
        FROM        sinfin.asignacion
        WHERE       "ENTIDAD_ID" = 'NATURGY' AND 'DATE2' >= '2024/12/01'
    """
    
    return text