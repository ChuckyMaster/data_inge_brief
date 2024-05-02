print("Hello, from script Py!")
import sqlite3
import time
import requests 
import pandas as pd
from io import StringIO


# Connexion à la base de données
conn = sqlite3.connect('/data/shop.db')
cursor = conn.cursor()

# Créer les tables
queries = [
    """CREATE TABLE IF NOT EXISTS shop(
           Id_shop INTEGER,
           city VARCHAR(50),
           employee_nb INT
       );""",
    """CREATE TABLE IF NOT EXISTS product(
           Id_product INTEGER PRIMARY KEY AUTOINCREMENT,
           name VARCHAR(50),
           id_reference_product VARCHAR(50),
           price D  ECIMAL(15,2),
           stock INT
       );""",
    """CREATE TABLE IF NOT EXISTS sale_by_product(
           Id_sale_by_product INTEGER PRIMARY KEY AUTOINCREMENT,
           ref_product_id VARCHAR(50),
           quantity INT
       );""",
    """CREATE TABLE IF NOT EXISTS sale_by_town(
           Id_sale_by_town INTEGER PRIMARY KEY AUTOINCREMENT,
           town VARCHAR(50),
           quantity INT
       );""",
    """CREATE TABLE IF NOT EXISTS sales_analytics(
           Id_sales_analytics INTEGER PRIMARY KEY AUTOINCREMENT,
           sale_revenue DECIMAL(15,2)
       );""",
    """CREATE TABLE IF NOT EXISTS sale(
           Id_shop INT,
           Id_product INT,
           Id_sales_analytics INT,
           Id_sale_by_town INT,
           Id_sale_by_product INT,
           created_at DATE,
           quantity INT,
           FOREIGN KEY(Id_shop) REFERENCES shop(Id_shop),
           FOREIGN KEY(Id_product) REFERENCES product(Id_product),
           FOREIGN KEY(Id_sales_analytics) REFERENCES sales_analytics(Id_sales_analytics),
           FOREIGN KEY(Id_sale_by_town) REFERENCES sale_by_town(Id_sale_by_town),
           FOREIGN KEY(Id_sale_by_product) REFERENCES sale_by_product(Id_sale_by_product)
       );"""
]

for query in queries:
    cursor.execute(query)
    print("Table created successfully.")

# Fonction pour insérer les données dans la table spécifiée à partir de l'URL donnée
def insert_data_from_url(url, table_name, column_names):
    # Récupération des données depuis l'URL et affichage en tant que DataFrame
    response = requests.get(url)
    if response.status_code == 200:
        # Utilisation de StringIO pour créer un objet de fichier en mémoire
        csv_data = StringIO(response.text)
        # Lecture du fichier CSV en excluant la première ligne
        df_data = pd.read_csv(csv_data, encoding='utf-8', skiprows=1)

        
        # Transformation du DataFrame en une liste de tuples
        data = [tuple(row) for row in df_data.itertuples(index=False)]
        
        # Connexion à la base de données SQLite existante
        conn = sqlite3.connect('/data/shop.db')
        cursor = conn.cursor()

        if table_name == 'sale':
            # Récupérer les données existantes dans la table sale
            cursor.execute("SELECT created_at, Id_product, quantity, Id_shop FROM sale;")
            existing_data = cursor.fetchall()

            # Filtrer les nouvelles données pour ne conserver que celles qui ne sont pas déjà dans la base de données
            new_data = [row for row in data if row not in existing_data]
            
            if new_data:
                # Requête SQL d'insertion avec 'executemany' pour les nouvelles données uniquement
                query = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({', '.join(['?'] * len(column_names))});"
                cursor.executemany(query, new_data)
                
                # Validation des changements dans la base de données
                conn.commit()
                
                print(f"New data inserted successfully into the '{table_name}' table.")
            else:
                print("No new data to insert into the 'sale' table.")
        else:
            # Requête SQL d'insertion avec 'executemany'
            query = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({', '.join(['?'] * len(column_names))});"
            cursor.executemany(query, data)
            
            # Validation des changements dans la base de données
            conn.commit()
            
            print(f"Data inserted successfully into the '{table_name}' table.")

        # Fermeture de la connexion à la base de données
        conn.close()
    else:
        print(f"Échec de récupération des données depuis l'URL : {url}")

# Définition des URLs et des noms de table avec leurs colonnes correspondantes
urls = {
    "product": "https://docs.google.com/spreadsheets/d/e/2PACX-1vSawI56WBC64foMT9pKCiY594fBZk9Lyj8_bxfgmq-8ck_jw1Z49qDeMatCWqBxehEVoM6U1zdYx73V/pub?gid=0&single=true&output=csv",
    "shop": "https://docs.google.com/spreadsheets/d/e/2PACX-1vSawI56WBC64foMT9pKCiY594fBZk9Lyj8_bxfgmq-8ck_jw1Z49qDeMatCWqBxehEVoM6U1zdYx73V/pub?gid=714623615&single=true&output=csv",
    "sale": "https://docs.google.com/spreadsheets/d/e/2PACX-1vSawI56WBC64foMT9pKCiY594fBZk9Lyj8_bxfgmq-8ck_jw1Z49qDeMatCWqBxehEVoM6U1zdYx73V/pub?gid=760830694&single=true&output=csv"
}

table_columns = {
    "product": ["name", "id_reference_product", "price", "stock"],
    "shop": ["Id_shop","city", "employee_nb"],
    "sale": ["created_at","Id_product", "quantity","Id_shop"]
}

# Insertion des données pour chaque table à partir des URLs
for table_name, url in urls.items():
    insert_data_from_url(url, table_name, table_columns[table_name])

import subprocess
subprocess.run(["python", "analyze_sql.py"])

# Boucle infinie pour maintenir le script en cours d'exécution
while True:
    # Sleep pour éviter une consommation excessive de CPU
    time.sleep(60)


