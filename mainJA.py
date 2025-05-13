import pandas as pd
import os

#1. **Representa** una clasificación del nº de clientes por estado (Si consideras que hay demasiados estados representa el top 5). 
# Paso siguiente crea una tabla donde se representen los estados, las ciudades que pertenecen a esos estados y el numero de clientes en esas ciudades. 
# Ademas de eso, la tabla y todos los graficos representados deberan de ser dinamicos respecto a la fecha

script_dir = os.path.dirname(__file__)

ruta_archivo = os.path.join(script_dir, 'recursos/Olist_Data/olist_orders_dataset.csv')
ruta_archivo2 = os.path.join(script_dir, 'recursos/Olist_Data/olist_customers_dataset.csv')
ruta_archivo3 = os.path.join(script_dir, 'recursos/Olist_Data/olist_order_items_dataset.csv')
ruta_archivo4 = os.path.join(script_dir, 'recursos/Olist_Data/olist_order_reviews_dataset.csv')

df_orders = pd.read_csv(ruta_archivo, encoding="utf-8")
df_customers = pd.read_csv(ruta_archivo2, encoding="utf-8")
df_items = pd.read_csv(ruta_archivo3, encoding="utf-8")
df_reviews = pd.read_csv(ruta_archivo4, encoding="utf-8")
#print(df_orders)
#print(df_customers)

# Filtrar las columnas relevantes
df_customers_filtrado = df_customers[["customer_id", "customer_city", "customer_state"]]
#print(df_customers_filtrado)

df_orders_filtrado = df_orders[["order_id", "customer_id", "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_estimated_delivery_date", "order_delivered_customer_date"]]
#print(df_orders_filtrado)

df_orders_customers = df_orders_filtrado.merge(df_customers_filtrado, left_on='customer_id', right_on='customer_id', how='left')
#print(df_orders_customers)

#Convertir fechas a tipo Date
df_orders_customers['order_purchase_timestamp'] = pd.to_datetime(df_orders_customers['order_purchase_timestamp'], errors='coerce')
df_orders_customers['order_approved_at'] = pd.to_datetime(df_orders_customers['order_approved_at'], errors='coerce')
df_orders_customers['order_delivered_carrier_date'] = pd.to_datetime(df_orders_customers['order_delivered_carrier_date'], errors='coerce')
df_orders_customers['order_estimated_delivery_date'] = pd.to_datetime(df_orders_customers['order_estimated_delivery_date'], errors='coerce')
df_orders_customers['order_delivered_customer_date'] = pd.to_datetime(df_orders_customers['order_delivered_customer_date'], errors='coerce')

print(df_orders_customers.isnull().any().any())
print(df_orders_customers.isnull().sum())

print(df_orders_customers.dtypes)

df_orders_customers['order_approved_at'] = df_orders_customers['order_approved_at'].fillna(pd.Timestamp('1900-12-31'))
#print(df_orders_customers['fecha_columna'])


'''
clientes_por_estado = df_orders_customers.groupby("customer_state")["customer_id"].nunique().sort_values(ascending=False)

# Top 5 estados
top_5_estados = clientes_por_estado.head(5)
print(top_5_estados)

df_top_estados = df_orders_customers[df_orders_customers['customer_state'].isin(top_5_estados.index)]

# Agrupar por estado y ciudad, contando clientes únicos
tabla_estado_ciudad = df_top_estados.groupby(["customer_state", "customer_city"])["customer_id"].nunique().reset_index()
tabla_estado_ciudad.rename(columns={"customer_id": "n_clientes"}, inplace=True)
print(tabla_estado_ciudad)
'''