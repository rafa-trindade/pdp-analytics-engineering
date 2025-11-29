import pandas as pd
import requests
import os
from pathlib import Path
from datetime import datetime, timedelta

# -----------------------------
# API FERIADOS - https://api.invertexto.com/api-feriados
# -----------------------------
START_DATE = "2030-01-01"
END_DATE = "2033-12-31"
UF = "GO"
API_KEY = "22137|SRHoo5bWW93micFMAvCc5fq8d60utocr"
SEEDS_PATH = "dbt/seeds/"
os.makedirs(SEEDS_PATH, exist_ok=True)


def get_feriados(year, uf="GO"):
    url = f"https://api.invertexto.com/v1/holidays/{year}?token={API_KEY}&state={uf}"
    resp = requests.get(url)
    if resp.status_code == 200:
        data = resp.json()
        return {f["date"]: f["name"] for f in data}
    else:
        print(f"Erro ao buscar feriados {year}: {resp.text}")
        return {}

def fetch_feriados(start_year, end_year, uf=UF):
    feriados = {}
    for ano in range(start_year, end_year + 1):
        feriados.update(get_feriados(ano, uf))
    return feriados

# -----------------------------
# Dimensão de datas
# -----------------------------
def generate_dim_date():
    feriados = fetch_feriados(start_year=2030, end_year=2032, uf=UF)

    datas = pd.date_range(start=START_DATE, end=END_DATE)

    dias_semana_pt = {
        "Monday": "Segunda-feira",
        "Tuesday": "Terça-feira",
        "Wednesday": "Quarta-feira",
        "Thursday": "Quinta-feira",
        "Friday": "Sexta-feira",
        "Saturday": "Sábado",
        "Sunday": "Domingo"
    }

    df = pd.DataFrame({
        "chave_data": datas.strftime("%Y%m%d").astype(int),
        "data": datas,
        "ano": datas.year,
        "mes": datas.month,
        "dia": datas.day,
        "nome_dia_semana": datas.strftime("%A").map(dias_semana_pt),
        "fim_de_semana": datas.weekday >= 5,
        "trimestre": datas.quarter,
        "alta_temporada": datas.month == 7,  # Julho
        "feriado": datas.strftime("%Y-%m-%d").isin(feriados.keys()),
        "nome_feriado": datas.strftime("%Y-%m-%d").map(feriados).fillna("")
    })

    df.to_csv(os.path.join(SEEDS_PATH, "dim_data.csv"), index=False)
    print(f"dim_data.csv gerado com {len(df)} linhas.")

# -----------------------------
# Dimensão de tempo
# -----------------------------
def generate_dim_time():
    tempos = pd.date_range("00:00", "23:59", freq="1min").time
    df = pd.DataFrame({
        "chave_hora": [t.hour * 100 + t.minute for t in tempos],
        "hora_24h": [t.strftime("%H:%M") for t in tempos],
        "hora": [t.hour for t in tempos],
        "minuto": [t.minute for t in tempos],
        "periodo": [
            "Manhã" if 5 <= t.hour <= 11 else
            "Tarde" if 12 <= t.hour <= 17 else
            "Noite" if 18 <= t.hour <= 23 else
            "Madrugada"
            for t in tempos
        ]
    })
    df.to_csv(os.path.join(SEEDS_PATH, "dim_tempo.csv"), index=False)
    print(f"dim_tempo.csv gerado com {len(df)} linhas.")

def process_pousada_despesas():
    input_path = Path("data/utils/pousada_despesas.xlsx")
    output_path = Path("dbt/seeds/dim_despesas.csv")
    
    df = pd.read_excel(input_path, sheet_name="despesas", engine="openpyxl")
    
    df['data_key'] = pd.to_datetime(df['data'], dayfirst=True).dt.strftime('%Y%m%d').astype(int)
    
    df['data'] = pd.to_datetime(df['data'], dayfirst=True).dt.strftime('%Y-%m-%d')
    
    if 'empresa' in df.columns:
        df = df.drop(columns=['empresa'])
    
    df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
    
    df.to_csv(output_path, index=False, sep=',', encoding='utf-8')
    
    print(f"dim_despesas.csv gerado com {len(df)} linhas.")


def generate_seeds():
    generate_dim_date()
    #generate_dim_time()
    process_pousada_despesas()


if __name__ == "__main__":
    generate_seeds()
