import pandas as pd
from datetime import date
from db.read import *
from db.connect import *

df = read_file()

list_columns = {
    'IDENTIFICACION_y' :'IDENTIFICACION',
    'GESTOR_ID' : 'GESTOR_ID',
    'HISTORY_DATE' : 'HISTORY_DATE',
    'ID_ACCION':'ID_ACCION',    
    'ID_EFECTO':'ID_EFECTO',
    'ID_CONTACTO':'ID_CONTACTO',
    'ID_SUBEFECTO':'ID_SUBEFECTO',
    'ID_REACCION':'ID_REACCION',
    'OBSERVACION':'OBSERVACION',
    'NEXT_ACTIVITY_DATE':'NEXT_ACTIVITY_DATE',
    'TELEPHONE':'TELEPHONE',
    'ACCOUNT_NUMBER':'ACCOUNT_NUMBER',
    'REASON_ID':'REASON_ID',
    'EMAIL_ADDRESS':'EMAIL_ADDRESS',
    'LETTER_ID':'LETTER_ID',
    'NOMBRE_CONTACTO':'NOMBRE_CONTACTO',
    'STATE_DEBTOR':'STATE_DEBTOR',
    'TEXT1_y':'TEXT1',
    'TEXT2_y':'TEXT2',
    'TEXT3_y':'TEXT3',
    'TEXT4_y':'TEXT4',
    'TEXT5_y':'TEXT5',
    'CANAL':'CANAL'
}
        
if df is None or df.empty:
    print('sin nuevos datos.')
else:
    df_masiv = pd.DataFrame(columns=list(list_columns.values()))   
    df_masiv['IDENTIFICACION'] = None
    df_masiv['HISTORY_DATE'] = df['created_at']
    df_masiv['ID_ACCION'] = 'SIN INFORMACION'
    df_masiv['ID_EFECTO'] = 'NO CONTESTA'
    df_masiv['ID_CONTACTO'] = 'NO CONTACTO'
    df_masiv['ID_SUBEFECTO'] = None
    df_masiv['ID_REACCION'] = None
    df_masiv['OBSERVACION'] = df['state']
    df_masiv['NEXT_ACTIVITY_DATE'] = None
    df_masiv['TELEPHONE'] = df['phone'].astype(str)
    df_masiv['ACCOUNT_NUMBER'] = df['Otra información 1'].astype(str)
    df_masiv['REASON_ID'] = None
    df_masiv['EMAIL_ADDRESS'] = None
    df_masiv['LETTER_ID'] = None
    df_masiv['NOMBRE_CONTACTO'] = None
    df_masiv['STATE_DEBTOR'] = None
    df_masiv['TEXT1'] = None
    df_masiv['TEXT2'] = None
    df_masiv['TEXT3'] = None
    df_masiv['TEXT4'] = None
    df_masiv['TEXT5'] = None
    df_masiv['CANAL'] = None
    df_masiv['GESTOR_ID'] = 'maquina'



def merge_db_dropcall():
    df_db = read_db()
    #df_masiv1 = pd.read_excel(r'Z:\1. Coordinadores\Asignaciones\Informes Lead\masivo_maquina.xlsx')
    #df_db['NUMERO_CUENTA'] = pd.to_numeric(df_db['NUMERO_CUENTA'], errors='coerce')
    
    df_merge = pd.merge(df_db, df_masiv, left_on='NUMERO_CUENTA', right_on='ACCOUNT_NUMBER', how='left', indicator=True)
    
    df_merge = df_merge[df_merge['_merge'] == 'both']
    df_merge['IDENTIFICACION_y'] = df_merge['IDENTIFICACION_x']
    existing_columns = df_merge.columns.intersection(list_columns.keys())
    df_merge = df_merge[existing_columns].rename(columns={col: list_columns[col] for col in existing_columns})

    return df_merge
   
def definition_date():
    date_today = date.today()
    return date_today
    
def read_db():
    text_query = read_data_db()
    engine_local = conect_db()
    df_db = pd.read_sql_query(text_query, engine_local)
    return (df_db)

df_save = merge_db_dropcall()
name_file = 'intentos_maquina_' + str(definition_date())
df_save.to_excel(fr'Z:\1. Coordinadores\Asignaciones\Informes Lead\{name_file}.xlsx', index=False)
eliminate_file()