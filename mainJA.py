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

df_payments_filtrado = df_payments[["order_id", "payment_value"]]

df_items_filtrado = df_items[["order_id"]]

df_reviews_filtrado = df_reviews[["order_id", "review_id", "review_score"]]

# Combinacion de los datasets, usando left join
df_orders_customers = df_orders_filtrado.merge(df_customers_filtrado, left_on='customer_id', right_on='customer_id', how='left')
df_orders_customers_payments = df_orders_customers.merge(df_payments_filtrado, left_on='order_id', right_on='order_id', how='left')
df_orders_customers_payments_items_review = df_orders_customers_payments.merge(df_items_filtrado, left_on='order_id', right_on='order_id', how='left')
#df_orders_customers_payments_items_review = df_orders_customers_payments_items.merge(df_reviews_filtrado, left_on='order_id', right_on='order_id', how='left')

# Eliminar duplicados solo si son exactamente el mismo pedido
df_orders_customers_payments_items_review.drop_duplicates()

# Suma de los distintos tipos de pago de cada pedido
payment_sum = df_orders_customers_payments_items_review.groupby('order_id')['payment_value'].sum()
# Eliminamos la columna original (para evitar duplicados en el merge)
df_orders_customers_payments_items_review = df_orders_customers_payments_items_review.drop(columns='payment_value')
# Hacemos el merge con la suma
df_orders_customers_payments_items_review = df_orders_customers_payments_items_review.merge(payment_sum, on='order_id', how='left')

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

# Conteo de nulos y eliminacion de duplicados
#print(df_orders_customers_payments_items_review.isnull().sum())
df_orders_customers_payments_items_review = df_orders_customers_payments_items_review.drop_duplicates()

# Rellenar fechas nulas con una fecha falsa, para su posterior deteccion en el analisis
df_orders_customers_payments_items_review['order_approved_at'] = df_orders_customers_payments_items_review['order_approved_at'].fillna(pd.Timestamp('1900-12-31'))
df_orders_customers_payments_items_review['order_delivered_carrier_date'] = df_orders_customers_payments_items_review['order_delivered_carrier_date'].fillna(pd.Timestamp('1900-12-31'))
df_orders_customers_payments_items_review['order_delivered_customer_date'] = df_orders_customers_payments_items_review['order_delivered_customer_date'].fillna(pd.Timestamp('1900-12-31'))

# Rellenado del único pedido con pago nulo
df_orders_customers_payments_items_review.loc[
    df_orders_customers_payments_items_review['order_id'] == 'bfbd0f9bdef84302105ad712db648a6c',
    ['payment_value']
] = [47.82]

# Calcular la diferencia y extraer los días
df_orders_customers_payments_items_review['dias_retraso_entrega'] = (
    df_orders_customers_payments_items_review['order_estimated_delivery_date'] - df_orders_customers_payments_items_review['order_delivered_customer_date']
).dt.days

# Si la diferencia es >= 0, poner 0; si es < 0, poner valor absoluto
df_orders_customers_payments_items_review['dias_retraso_entrega'] = df_orders_customers_payments_items_review['dias_retraso_entrega'].apply(lambda x: 0 if x >= 0 else abs(x))

df_retrasos = df_orders_customers_payments_items_review[df_orders_customers_payments_items_review['dias_retraso_entrega'] > 0]
#print(df_retrasos.head(30))

# Formatear todas las columnas de fecha al mismo formato: "YYYY-MM-DD HH:MM:SS"
#formato_fecha = "%Y-%m-%d %H:%M:%S"

#for columna in columnas_fecha:
    #df_orders_customers_payments_items_review[columna] = df_orders_customers_payments_items_review[columna].dt.strftime(formato_fecha)

#print(df_orders_customers_payments_items_review[df_orders_customers_payments_items_review["payment_value"].isnull()])
#print(df_orders_customers_payments_items_review.isnull().any().any())
#print(df_orders_customers_payments_items_review.isnull().sum())
#print(df_orders_customers_payments_items_review)

#es_unico = df_orders_customers_payments_items_review['order_id'].is_unique
#print(f"¿Todos los order_id son únicos?: {es_unico}")
#duplicados = df_orders_customers_payments_items_review['order_id'].duplicated().sum()
#print(f"Número de order_id duplicados: {duplicados}")
#order_id_duplicados = df_orders_customers_payments_items_review['order_id'][df_orders_customers_payments_items_review['order_id'].duplicated()]
#print(order_id_duplicados)

pedidos_por_cliente = df_orders_customers_payments_items_review.groupby("customer_id")["order_id"].count()
# Mostrar los resultados, asegurando que se ve la distribución de pedidos por cliente
print(pedidos_por_cliente.value_counts().sort_index())
#print(pedidos_por_cliente.head()) 
#df_orders_customers_payments_items_review.to_csv('df_orders_customs_payments_items_review.csv', index=False)

'''
#---------------------------------------------------------------------------
import streamlit as st
import plotly.express as px

# Cargar datos
df = df_orders_customers_payments_items_review

# Filtrar por rango de fechas
st.title("Análisis de Clientes por Estado y Ciudad")

fecha_min = df['order_purchase_timestamp'].min()
fecha_max = df['order_purchase_timestamp'].max()

rango_fechas = st.date_input("Selecciona el rango de fechas:", [fecha_min, fecha_max])

if len(rango_fechas) == 2:
    inicio, fin = pd.to_datetime(rango_fechas)
    df = df[(df['order_purchase_timestamp'] >= inicio) & (df['order_purchase_timestamp'] <= fin)]

    # Calculo de clientes por estado
    clientes_por_estado = df.groupby("customer_state")['customer_id'].nunique().sort_values(ascending=False)
    top_5_estados = clientes_por_estado.head(5)

    st.subheader("Top 5 Estados por Número de Clientes")
    fig_top_estados = px.bar(top_5_estados, x=top_5_estados.index, y=top_5_estados.values,
                              labels={'x': 'Estado', 'y': 'Nº de Clientes'}, color=top_5_estados.index)
    st.plotly_chart(fig_top_estados)
    
    # Filtros dinámicos por estado y ciudad (top 10 ciudades con más clientes)
    estados_seleccionados = st.multiselect("Selecciona los estados:", options=top_5_estados.index.tolist(), default=top_5_estados.index.tolist())
    df_top_estados = df[df['customer_state'].isin(estados_seleccionados)]
    
    top_10_ciudades = df_top_estados.groupby("customer_city")['customer_id'].nunique().sort_values(ascending=False).head(10).index.tolist()
    ciudades_seleccionadas = st.multiselect("Selecciona las ciudades:", options=top_10_ciudades, default=top_10_ciudades)

    df_top_estados = df_top_estados[df_top_estados['customer_city'].isin(ciudades_seleccionadas)]

    total_pedidos = df_top_estados['order_id'].nunique()

    df_top_estados_clean = df_top_estados.drop_duplicates(subset=["order_id", "customer_id"])
   # Calcular número de pedidos por cliente
    df_ratio = df_top_estados_clean.groupby(["customer_state", "customer_city", "customer_id"])["order_id"].count().reset_index()
    df_ratio.rename(columns={"order_id": "n_pedidos"}, inplace=True)

    # Agrupar para calcular métricas finales
    df_ratio_final = df_ratio.groupby(["customer_state", "customer_city"]).agg(
        n_clientes=("customer_id", "nunique"),
        n_pedidos=("n_pedidos", "sum")
    ).reset_index()

    df_ratio_final["porcentaje_pedidos"] = (df_ratio_final["n_pedidos"] / total_pedidos * 100).round(2)
    df_ratio_final["ratio_pedidos_cliente"] = (df_ratio_final["n_pedidos"] / df_ratio_final["n_clientes"]).round(2)

    st.subheader("Tabla de Clientes, Pedidos y Ratios por Ciudad")
    st.dataframe(df_ratio_final.sort_values(by=["customer_state", "n_clientes"], ascending=[True, False]))

    st.subheader("Pedidos por Cliente (Ratio) - Ciudades Seleccionadas")
    fig_ratio = px.bar(
        df_ratio_final,
        x="customer_city",
        y="ratio_pedidos_cliente",
        color="customer_state",
        barmode="group",
        labels={"ratio_pedidos_cliente": "Ratio de Pedidos por Cliente", "customer_city": "Ciudad"},
        title="Ratio de Pedidos por Cliente en las Ciudades Seleccionadas"
    )
    st.plotly_chart(fig_ratio)

else:
    st.warning("Selecciona un rango de fechas válido para visualizar los datos.")
'''