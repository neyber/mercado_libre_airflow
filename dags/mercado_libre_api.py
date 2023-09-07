import requests
import psycopg2
from datetime import datetime

def fetch_categories_data():
    api_url = 'https://api.mercadolibre.com/sites/MLA/categories'
    response = requests.get(api_url)
    
    if response.status_code == 200:
        categories = response.json()
        return categories
    else:
        raise Exception(f"Failed to retrieve data. Status code: {response.status_code}")
    
def insert_data_to_postgres():
    data = fetch_categories_data()
    
    conn = psycopg2.connect(
        dbname = 'mercado_libre',
        user = 'airflow',
        password = 'airflow',
        host = 'postgres',
        port = '5432'
    )

    cur = conn.cursor()

    insert_query = """
        INSERT INTO dmstage.Categories (id, name, load_date)
        VALUES (%s, %s, %s);
    """
    
    current_datetime = datetime.now()

    for row in data:
        cur.execute(insert_query, (row['id'], row['name'], current_datetime))

    conn.commit()
    cur.close()
    conn.close()