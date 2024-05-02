import sqlite3

# Connexion à la base de données
conn = sqlite3.connect('/data/shop.db')
cursor = conn.cursor()

# Requête pour obtenir le chiffre d'affaires total
cursor.execute("SELECT SUM(p.price * s.quantity) AS chiffre_affaire FROM sale s JOIN product p ON s.id_product = p.id_reference_product;")
total_revenue = cursor.fetchone()[0]
print("Chiffres d'affaire:", total_revenue)

# Requête pour obtenir les ventes par produit
cursor.execute("SELECT id_product AS p, SUM(quantity) AS total_sales FROM sale GROUP BY p;")
sales_per_product = cursor.fetchall()
print("Vente par produit:", sales_per_product)

# Requête pour obtenir les ventes par région
cursor.execute("SELECT shop.city, SUM(sale.quantity) AS total_quantity FROM sale INNER JOIN shop ON sale.Id_shop = shop.Id_shop GROUP BY shop.city;")
sales_per_region = cursor.fetchall()
print("Vente par ville:", sales_per_region)


# Pour la table sale_by_product
for product_id, quantity in sales_per_product:
    cursor.execute("SELECT * FROM sale_by_product WHERE ref_product_id = ?;", (product_id,))
    existing_entry = cursor.fetchone()
    if existing_entry is None:
        cursor.execute("INSERT INTO sale_by_product (ref_product_id, quantity) VALUES (?, ?);", (product_id, quantity))
        conn.commit()

# Pour la table sale_by_town
for town, quantity in sales_per_region:
    cursor.execute("SELECT * FROM sale_by_town WHERE town = ?;", (town,))
    existing_entry = cursor.fetchone()
    if existing_entry is None:
        cursor.execute("INSERT INTO sale_by_town (town, quantity) VALUES (?, ?);", (town, quantity))
        conn.commit()

# Pour la table sales_analytics
cursor.execute("SELECT * FROM sales_analytics;")
existing_entry = cursor.fetchone()
if existing_entry is None:
    cursor.execute("INSERT INTO sales_analytics (sale_revenue) VALUES (?);", (total_revenue,))
    conn.commit()


# Fermeture de la connexion à la base de données
conn.close()
 