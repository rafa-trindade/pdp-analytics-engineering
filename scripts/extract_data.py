import os
import pandas as pd
import pyodbc
from config.db_config import SQL_SERVER_CONFIG

def extract_from_sqlserver():
    tables = [
        "clsCliente",
        "clsEmpresa",
        "clsHospedagem",
        "clsProduto",
        "clsConsumacao",
        "clsApartamento"
    ]
    
    output_dir = "/opt/airflow/data/extracted"
    
    os.makedirs(output_dir, exist_ok=True)
    
    conn_str = (
        f"DRIVER={{{SQL_SERVER_CONFIG['driver']}}};"
        f"SERVER={SQL_SERVER_CONFIG['server']};"
        f"DATABASE={SQL_SERVER_CONFIG['database']};"
        f"UID={SQL_SERVER_CONFIG['user']};"
        f"PWD={SQL_SERVER_CONFIG['password']}"
    )
    
    try:
        conn = pyodbc.connect(conn_str)
        print("Conexão com o SQL Server estabelecida com sucesso!")
        
        for table in tables:
            print(f"📥 Extraindo tabela: {table} ...")
            query = f"SELECT * FROM {table};"
            df = pd.read_sql(query, conn)
            
            file_name = f"{table}.csv"
            file_path = os.path.join(output_dir, file_name)
            
            df.to_csv(file_path, index=False, encoding="utf-8-sig")
            print(f"{table} salva em: {file_path} (linhas: {len(df)})")
        
        conn.close()
        print("Extração concluída com sucesso!")
        print(f"Arquivos disponíveis em: {output_dir}")
    
    except Exception as e:
        print(f"Erro na extração: {e}")
        raise

if __name__ == "__main__":
    extract_from_sqlserver()
