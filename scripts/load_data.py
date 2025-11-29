import os
import pandas as pd
from sqlalchemy import create_engine, text
from config.db_config import POSTGRES_CONFIG

DATA_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'extracted')
RAW_SCHEMA = "raw"

def get_engine():
    cfg = POSTGRES_CONFIG
    conn_str = f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    return create_engine(conn_str)

def ensure_schema_exists(engine, schema_name):
    """Cria o schema staging se não existir."""
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name};"))

def load_csv_to_postgres(engine, csv_path):
    table_name = os.path.splitext(os.path.basename(csv_path))[0].lower()
    full_table_name = f'{RAW_SCHEMA}."{table_name}"'
    print(f"\n🔹 Processando {csv_path} → tabela {full_table_name}")

    df = pd.read_csv(csv_path)
    if df.empty:
        print(f"Arquivo {csv_path} está vazio. Pulando...")
        return

    id_col = None
    for c in df.columns:
        if c.lower() in ("id", "codigo", "chave", "uuid"):
            id_col = c
            break
    if id_col is None:
        raise ValueError(f"Nenhuma coluna de ID encontrada no CSV {csv_path}")

    with engine.begin() as conn:
        columns_def = ", ".join([f'"{c}" TEXT' for c in df.columns])
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            {columns_def},
            PRIMARY KEY ("{id_col}")
        );
        """
        conn.execute(text(create_sql))

        tmp_path = f"/tmp/{table_name}.csv"
        df.to_csv(tmp_path, index=False, header=False)

        with conn.connection.cursor() as cur:
            with open(tmp_path, "r", encoding="utf-8") as f:
                cur.copy_expert(
                    f"""
                    COPY {full_table_name} ({', '.join([f'"{c}"' for c in df.columns])})
                    FROM STDIN WITH CSV;
                    """,
                    f
                )
            conn.connection.commit()

        print(f"Carga de {len(df)} linhas concluída → {full_table_name}")

def main():
    engine = get_engine()
    ensure_schema_exists(engine, RAW_SCHEMA) 

    if not os.path.exists(DATA_PATH):
        print(f"Pasta {DATA_PATH} não encontrada.")
        return

    csv_files = [os.path.join(DATA_PATH, f) for f in os.listdir(DATA_PATH) if f.endswith(".csv")]
    if not csv_files:
        print(f"Nenhum CSV encontrado em {DATA_PATH}.")
        return

    for csv_file in csv_files:
        try:
            load_csv_to_postgres(engine, csv_file)
        except Exception as e:
            print(f"Erro ao processar {csv_file}: {e}")

if __name__ == "__main__":
    main()
