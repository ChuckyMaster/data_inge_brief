import sqlite3

# Connexion à la base de données
conn = sqlite3.connect('/data/shop.db')
cursor = conn.cursor()

# Requête pour obtenir le chiffre d'affaires total
cursor.execute("SELECT SUM(p.price * s.quantity) AS chiffre_affaire FROM sale s JOIN product p ON s.id_product = p.id_reference_product;")
total_revenue = cursor.fetchone()[0]
print("Total revenue:", total_revenue)

# Requête pour obtenir les ventes par produit
cursor.execute("SELECT id_product AS p, SUM(quantity) AS total_sales FROM sale GROUP BY p;")
sales_per_product = cursor.fetchall()
print("Sales per product:", sales_per_product)

# Requête pour obtenir les ventes par région
cursor.execute("SELECT shop.city, SUM(sale.quantity) AS total_quantity FROM sale INNER JOIN shop ON sale.Id_shop = shop.Id_shop GROUP BY shop.city;")
sales_per_region = cursor.fetchall()
print("Sales per region:", sales_per_region)

# Fermeture de la connexion à la base de données
conn.close()
