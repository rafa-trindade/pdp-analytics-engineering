import os
from dotenv import load_dotenv

load_dotenv()

# SQL Server container
SQL_SERVER_CONFIG = {
    "driver": os.getenv("SQLSERVER_DRIVER"),
    "server": os.getenv("SQLSERVER_SERVER"),
    "database": os.getenv("SQLSERVER_DATABASE"),
    "user": os.getenv("SQLSERVER_USER"),
    "password": os.getenv("SQLSERVER_PASSWORD")
}

# PostgreSQL
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),  
    "port": os.getenv("POSTGRES_PORT"),  
    "database": os.getenv("POSTGRES_DB"), 
    "user": os.getenv("POSTGRES_USER"), 
    "password": os.getenv("POSTGRES_PASSWORD") 
}
