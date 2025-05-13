import pandas as pd
import os
pd.set_option('display.max_columns', None)

#1. **Representa** una clasificación del nº de clientes por estado (Si consideras que hay demasiados estados representa el top 5). 
# Paso siguiente crea una tabla donde se representen los estados, las ciudades que pertenecen a esos estados y el numero de clientes en esas ciudades. 
# Ademas de eso, la tabla y todos los graficos representados deberan de ser dinamicos respecto a la fecha

script_dir = os.path.dirname(__file__)

ruta_archivo = os.path.join(script_dir, 'recursos/Olist_Data/olist_orders_dataset.csv')
ruta_archivo2 = os.path.join(script_dir, 'recursos/Olist_Data/olist_customers_dataset.csv')
ruta_archivo3 = os.path.join(script_dir, 'recursos/Olist_Data/olist_order_items_dataset.csv')
ruta_archivo4 = os.path.join(script_dir, 'recursos/Olist_Data/olist_order_reviews_dataset.csv')
ruta_archivo5 = os.path.join(script_dir, 'recursos/Olist_Data/olist_order_payments_dataset.csv')

df_orders = pd.read_csv(ruta_archivo, encoding="utf-8")
df_customers = pd.read_csv(ruta_archivo2, encoding="utf-8")
df_items = pd.read_csv(ruta_archivo3, encoding="utf-8")
df_reviews = pd.read_csv(ruta_archivo4, encoding="utf-8")
df_payments = pd.read_csv(ruta_archivo5, encoding="utf-8")

# Filtrar las columnas relevantes
df_customers_filtrado = df_customers[["customer_id", "customer_city", "customer_state"]]

df_orders_filtrado = df_orders[["order_id", "customer_id", "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_estimated_delivery_date", "order_delivered_customer_date"]]

df_payments_filtrado = df_payments[["order_id", "payment_sequential", "payment_value"]]

df_items_filtrado = df_items[["order_id", "shipping_limit_date"]]

df_reviews_filtrado = df_reviews[["order_id", "review_id", "review_score"]]

# Combinacion de los datasets, usando left join
df_orders_customers = df_orders_filtrado.merge(df_customers_filtrado, left_on='customer_id', right_on='customer_id', how='left')
df_orders_customers_payments = df_orders_customers.merge(df_payments_filtrado, left_on='order_id', right_on='order_id', how='left')
df_orders_customers_payments_items = df_orders_customers_payments.merge(df_items_filtrado, left_on='order_id', right_on='order_id', how='left')
df_orders_customers_payments_items_review =df_orders_customers_payments_items.merge(df_reviews_filtrado, left_on='order_id', right_on='order_id', how='left')

# Convertir columnas de fecha a tipo datetime
columnas_fecha = [
    'order_purchase_timestamp',
    'order_approved_at',
    'order_delivered_carrier_date',
    'order_delivered_customer_date',
    'order_estimated_delivery_date'
]

for columna in columnas_fecha:
    df_orders_customers_payments_items_review[columna] = pd.to_datetime(df_orders_customers_payments_items_review[columna], errors='coerce')

# Calcula el año de la primera compra de cada cliente y lo añade al dataframe original
primer_año_por_cliente = (
    df_orders_customers_payments_items_review.groupby('customer_id')['order_purchase_timestamp']
    .min()
    .dt.year
    .rename('primer_año_compra')
)
df_orders_customers_payments_items_review = df_orders_customers_payments_items_review.merge(primer_año_por_cliente, on='customer_id', how='left')

# Formatear todas las columnas de fecha al mismo formato: "YYYY-MM-DD HH:MM:SS"
formato_fecha = "%Y-%m-%d %H:%M:%S"

for columna in columnas_fecha:
    df_orders_customers_payments_items_review[columna] = df_orders_customers_payments_items_review[columna].dt.strftime(formato_fecha)

# Conteo de nulos y eliminacion de duplicados
print(df_orders_customers_payments_items_review.isnull().any().any())
print(df_orders_customers_payments_items_review.isnull().sum())
df_orders_customers_payments_items_review = df_orders_customers_payments_items_review.drop_duplicates()

# Rellenar fechas nulas con una fecha falsa, para su posterior deteccion en el analisis
df_orders_customers_payments_items_review['order_approved_at'] = df_orders_customers_payments_items_review['order_approved_at'].fillna(pd.Timestamp('1900-12-31'))
df_orders_customers_payments_items_review['order_delivered_carrier_date'] = df_orders_customers_payments_items_review['order_delivered_carrier_date'].fillna(pd.Timestamp('1900-12-31'))
df_orders_customers_payments_items_review['order_delivered_customer_date'] = df_orders_customers_payments_items_review['order_delivered_customer_date'].fillna(pd.Timestamp('1900-12-31'))

print(df_orders_customers_payments_items_review.isnull().any().any())
print(df_orders_customers_payments_items_review.isnull().sum())
print(df_orders_customers_payments_items_review)


'''
# Calculo de clientes por estado
clientes_por_estado = df_orders_customers.groupby("customer_state")["customer_id"].nunique().sort_values(ascending=False)
# Top 5 estados con mas clientes
top_5_estados = clientes_por_estado.head(5)
print(top_5_estados)

df_top_estados = df_orders_customers[df_orders_customers['customer_state'].isin(top_5_estados.index)]

# Agrupar por estado y ciudad, contando clientes
tabla_estado_ciudad = df_top_estados.groupby(["customer_state", "customer_city"])["customer_id"].count().reset_index()
tabla_estado_ciudad.rename(columns={"customer_id": "n_clientes"}, inplace=True)
print(tabla_estado_ciudad)
#print(tabla_estado_ciudad[tabla_estado_ciudad['customer_city'] == "sao paulo"])
print(df_orders_customers.dtypes)

# Calcula el año de la primera compra de cada cliente y lo añade al dataframe original
primer_año_por_cliente = (
    df_orders_customers.groupby('customer_id')['order_purchase_timestamp']
    .min()
    .dt.year
    .rename('primer_año_compra')
)
df_orders_customers = df_orders_customers.merge(primer_año_por_cliente, on='customer_id', how='left')
print(df_orders_customers)


#Creacion del formulario con filtros para el estado, ciudad y año de compra
import streamlit as st

# Título de la app
st.title("Formulario de Registro")

# Usamos un formulario con 'with' para agrupar los inputs
with st.form("formulario_registro"):
    estado = st.text_input("Estado")
    ciudad = st.text_input("Ciudad")
    año = st.number_input("Año", min_value=0, max_value=120, step=1)
    genero = st.selectbox("Género", ["Seleccione...", "Masculino", "Femenino", "Otro"])
    acepta = st.checkbox("Acepto los términos y condiciones")

    # Botón para enviar el formulario
    enviar = st.form_submit_button("Enviar")

'''
