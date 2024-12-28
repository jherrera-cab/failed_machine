from dotenv import load_dotenv
from sqlalchemy import create_engine
import os

def conect_db():
    load_dotenv()
    engine=None
    engine_local = None
    try:
        engine_local = create_engine(f"postgresql://{os.getenv('usuario_local')}:{os.getenv('password_local')}@{os.getenv('host_local')}/{os.getenv('name_DB_local')}")
       
        if engine.connect():
            print("Conexion exitosa en la base de datos Sinfin.")
                     
        if engine_local.connect():
            print("Conexion exitosa en la base de datos Local.")
        
    except Exception as e:
        print("Error en la conexion de la base de datos de datos:", e)
    

    return engine_local