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

# Combinación de datasets para el ejercicio 4
df_review_orders = df_reviews.merge(
    df_orders[["order_id", "customer_id"]],
    on="order_id",
    how="left"
)

df_reviews_full = df_review_orders.merge(
    df_customers[["customer_id", "customer_state"]],
    on="customer_id",
    how="left"
)

df_reviews_full = df_reviews_full.merge(
    df_orders_customers_payments_items_review[["order_id", "dias_retraso_entrega", "payment_value"]],
    on="order_id",
    how="left"
)

# Filtrar solo los pedidos sin retraso
df_reviews_full_sin_retraso = df_reviews_full[df_reviews_full["dias_retraso_entrega"] == 0]

# Seleccionar columnas finales
df_ejercicio4 = df_reviews_full_sin_retraso[[
    "customer_state", "review_id", "review_score", "dias_retraso_entrega", "payment_value", "order_id", "customer_id"
]]

# Mostrar resultados
#df_ejercicio4.to_csv('df_ejercicio4.csv', index=False)
#print(df_ejercicio4.head(50))

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

#pedidos_por_cliente = df_orders_customers_payments_items_review.groupby("customer_id")["order_id"].count()
# Mostrar los resultados, asegurando que se ve la distribución de pedidos por cliente
#print(pedidos_por_cliente.value_counts().sort_index())

#Guardar resultados en un csv
#df_orders_customers_payments_items_review.to_csv('df_orders_customs_payments_items_review.csv', index=False)


#---------------------------------------------------------------------------
import streamlit as st
import plotly.express as px
import matplotlib.pyplot as plt
import plotly.graph_objects as go

# Cargar datos
df = df_orders_customers_payments_items_review

# Filtrar por rango de fechas
st.title("Análisis de Olist Data-E Commerce")

fecha_min = df['order_purchase_timestamp'].min()
fecha_max = df['order_purchase_timestamp'].max()
numCiudades = st.number_input("Indique el número de ciudades por estado que desea: ", step= 1)

rango_fechas = st.date_input("Selecciona el rango de fechas:", [fecha_min, fecha_max])

if len(rango_fechas) == 2:
    inicio, fin = pd.to_datetime(rango_fechas)

    # Filtrar por rango de fechas
    df_filtrado_fecha = df[
        (df['order_purchase_timestamp'] >= inicio) &
        (df['order_purchase_timestamp'] <= fin)
    ]

    # Top 5 estados con más clientes
    top_5_estados = df_filtrado_fecha.groupby("customer_state")['customer_id'].nunique().sort_values(ascending=False).head(5)

    # Filtros dinámicos por estado y ciudad
    estados_seleccionados = st.multiselect("Selecciona los estados:", options=top_5_estados.index.tolist(), default=top_5_estados.index.tolist())
    df_top_estados = df_filtrado_fecha[df_filtrado_fecha['customer_state'].isin(estados_seleccionados)]

    top_ciudades = df_top_estados.groupby("customer_city")['customer_id'].nunique().sort_values(ascending=False).head(numCiudades).index.tolist()
    ciudades_seleccionadas = st.multiselect("Selecciona las ciudades:", options=top_ciudades, default=top_ciudades)

    clientes_estado_ciudad = df_top_estados[df_top_estados['customer_city'].isin(ciudades_seleccionadas)]

    # Tabla con métricas por ciudad
    agrupado = clientes_estado_ciudad.groupby(['customer_state', 'customer_city'])
    clientes_por_ciudad = agrupado['customer_id'].nunique().reset_index(name='num_clientes')
    pedidos_por_ciudad = agrupado['order_id'].nunique().reset_index(name='num_pedidos')

    tabla = pd.merge(clientes_por_ciudad, pedidos_por_ciudad, on=['customer_state', 'customer_city'])
    tabla['porcentaje_pedidos'] = (tabla['num_pedidos'] / df_filtrado_fecha['order_id'].nunique()) * 100
    tabla['ratio_pedidos_cliente'] = tabla['num_pedidos'] / tabla['num_clientes']

    st.dataframe(tabla)

    # Generar visualizaciones
    clientes_estado_agrupado = clientes_estado_ciudad.groupby("customer_state")['customer_id'].nunique().reset_index(name="num_clientes")
    clientes_estado_agrupado = clientes_estado_agrupado.sort_values(by="num_clientes", ascending=False)
    fig_clientes_estado = px.bar(
        clientes_estado_agrupado,
        x="customer_state",
        y="num_clientes",
        color="customer_state",
        title="Número de clientes por estado",
        category_orders={"customer_state": clientes_estado_agrupado["customer_state"].tolist()}
    )

    fig_ratio = px.scatter(
        tabla, x="customer_city", y="ratio_pedidos_cliente", color="customer_state",
        size="num_clientes", hover_data=["num_pedidos", "porcentaje_pedidos"],
        title="Ratio de pedidos por cliente por ciudad")

    pedidos_clientes = clientes_estado_ciudad.groupby(["customer_state", "customer_city"]).agg(
        count_orders=("order_id", "count"), count_customers=("customer_id", "nunique")
    ).reset_index().sort_values(by="count_orders", ascending=False)

    fig_bar_clientes_estado = px.bar(
        pedidos_clientes, x="customer_state", y="count_customers", color="customer_city",
        title="Clientes por ciudad y estado")

    df_ejercicio4_filtrado = df_ejercicio4[df_ejercicio4['customer_state'].isin(estados_seleccionados)]
    review_state = df_ejercicio4_filtrado.groupby("customer_state").agg(
        num_review=("review_id", "count"), mean_score=("review_score", "mean")
    ).reset_index().sort_values(by="mean_score", ascending=False)

    fig_review = go.Figure()
    fig_review.add_bar(x=review_state["customer_state"], y=review_state["num_review"], name="Número de reviews", marker_color="teal", yaxis="y1")
    fig_review.add_trace(go.Scatter(x=review_state["customer_state"], y=review_state["mean_score"], name="Puntuación media", mode="lines+markers", marker_color="maroon", yaxis="y2"))
    fig_review.update_layout(title="Reviews por estado", xaxis=dict(title="Estado"), yaxis=dict(title="Número de reviews"), yaxis2=dict(title="Puntuación media", overlaying="y", side="right", range=[0, 5]))

    pedidos_tarde = clientes_estado_ciudad.groupby("customer_city").agg(
        cantidad_pedidos_tarde=("dias_retraso_entrega", lambda x: (x > 0).sum()),
        media_dias_tarde=("dias_retraso_entrega", lambda x: x[x > 0].mean()),
        num_pedidos=("order_id", "count")
    ).reset_index().sort_values(by="cantidad_pedidos_tarde", ascending=False).head(20)
    pedidos_tarde["porcentaje_tarde"] = round(pedidos_tarde.cantidad_pedidos_tarde / pedidos_tarde.num_pedidos * 100, 2)

    fig_retrasos = go.Figure()
    fig_retrasos.add_trace(go.Bar(x=pedidos_tarde["customer_city"], y=pedidos_tarde["cantidad_pedidos_tarde"], name="Pedidos Tarde", marker_color='mediumseagreen'))
    fig_retrasos.add_trace(go.Bar(x=pedidos_tarde["customer_city"], y=pedidos_tarde["num_pedidos"] - pedidos_tarde["cantidad_pedidos_tarde"], name="Pedidos a Tiempo", marker_color='skyblue'))
    fig_retrasos.update_layout(barmode='stack', title="Pedidos tarde vs a tiempo por ciudad", xaxis_title="Ciudad", yaxis_title="Cantidad de pedidos")

    fig_media_retraso = px.bar(
        pedidos_tarde.sort_values(by="media_dias_tarde", ascending=True),
        x="media_dias_tarde", y="customer_city", orientation="h",
        title="Media de días de retraso por ciudad", color="media_dias_tarde", color_continuous_scale="Reds")

    tabla_estado_ciudad = df_top_estados.groupby(["customer_state", "customer_city"])["customer_id"].count().reset_index()
    tabla_estado_ciudad.rename(columns={"customer_id": "n_clientes"}, inplace=True)

    # Pestañas
    tab_clientes, tab_pedidos, tab_reviews = st.tabs(["📊 Clientes", "📦 Pedidos", "⭐ Reviews"])

    with tab_clientes:
        st.plotly_chart(fig_clientes_estado, use_container_width=True)

        st.subheader("Top ciudades con más clientes por estado")
        for i, estado in enumerate(estados_seleccionados):
            top_ciudades_estado = (
                tabla_estado_ciudad[tabla_estado_ciudad['customer_state'] == estado]
                .sort_values(by="n_clientes", ascending=False).head(int(numCiudades))
            )
            fig_bar = px.bar(
                top_ciudades_estado, x="n_clientes", y="customer_city", orientation="h",
                title=f"Top {numCiudades} ciudades por clientes en {estado}", color="n_clientes", color_continuous_scale="Blues"
            )
            st.plotly_chart(fig_bar, use_container_width=True, key=f"bar_{estado}_{i}")

        st.subheader("Dispersión de clientes por estado y ciudad")
        fig_scatter = px.scatter(
            tabla_estado_ciudad[tabla_estado_ciudad["customer_state"].isin(estados_seleccionados)],
            x="customer_state", y="customer_city", size="n_clientes", color="customer_state",
            title="Distribución de clientes por estado y ciudad", hover_data=["n_clientes"]
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

    with tab_pedidos:
        st.plotly_chart(fig_ratio, use_container_width=True)
        st.plotly_chart(fig_bar_clientes_estado, use_container_width=True)
        st.plotly_chart(fig_retrasos, use_container_width=True)
        st.plotly_chart(fig_media_retraso, use_container_width=True)

    with tab_reviews:
        st.plotly_chart(fig_review, use_container_width=True)


else:
    st.warning("Selecciona un rango de fechas válido para visualizar los datos.")
