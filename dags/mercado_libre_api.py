import requests

def fetch_categories_data():
    api_url = "https://api.mercadolibre.com/sites/MLA/categories"
    response = requests.get(api_url)
    
    if response.status_code == 200:
        categories = response.json()
        return categories
    else:
        raise Exception(f"Failed to retrieve data. Status code: {response.status_code}")