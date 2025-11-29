from scripts import dbt_seeds

def run_etl():
    print("🧩 Gerando seed (dim_data)")
    dbt_seeds.generate_seeds()
    print("✅ Seed gerado com sucesso!")

if __name__ == "__main__":
    run_etl()
