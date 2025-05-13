import pandas as pd
import os
import matplotlib.pyplot as plt

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
#print(df_customers_filtrado)

df_orders_filtrado = df_orders[["order_id", "customer_id", "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_estimated_delivery_date", "order_delivered_customer_date"]]
#print(df_orders_filtrado)

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

# Calculo de clientes por estado
clientes_por_estado = df_orders_customers.groupby("customer_state")["customer_id"].nunique().sort_values(ascending=False)
# Top 5 estados con mas clientes
top_5_estados = clientes_por_estado.head(5)
print(top_5_estados)
 
df_top_estados = df_orders_customers[df_orders_customers['customer_state'].isin(top_5_estados.index)]
 
#1. Agrupar por estado y ciudad, contando clientes
tabla_estado_ciudad = df_top_estados.groupby(["customer_state", "customer_city"])["customer_id"].count().reset_index()
tabla_estado_ciudad.rename(columns={"customer_id": "n_clientes"}, inplace=True)
print(tabla_estado_ciudad)

state_SP = tabla_estado_ciudad[tabla_estado_ciudad['customer_state']=="SP"].sort_values(by = "n_clientes", ascending=False).head(10)

state_RJ = tabla_estado_ciudad[tabla_estado_ciudad['customer_state']=="RJ"].sort_values(by = "n_clientes", ascending=False).head(10)
state_MG = tabla_estado_ciudad[tabla_estado_ciudad['customer_state']=="MG"].sort_values(by = "n_clientes", ascending=False).head(10)
state_PR = tabla_estado_ciudad[tabla_estado_ciudad['customer_state']=="PR"].sort_values(by = "n_clientes", ascending=False).head(10)
state_RS = tabla_estado_ciudad[tabla_estado_ciudad['customer_state']=="RS"].sort_values(by = "n_clientes", ascending=False).head(10)

# fig, ax = plt.subplots(2,3)
# ax[0,0].bar(tabla_estado_ciudad["customer_state"], tabla_estado_ciudad["n_clientes"])
# ax[0,1].pie(state_SP['n_clientes'], labels = state_SP["customer_city"])
# ax[0,2].bar(state_RJ["customer_city"], state_RJ['n_clientes'], label = "cities from RJ")
# ax[1,0].bar(state_MG["customer_city"], state_MG['n_clientes'], label = "cities from MG")
# ax[1,1].bar(state_PR["customer_city"], state_PR['n_clientes'], label = "cities from PR")
# ax[1,2].bar(state_RS["customer_city"], state_RS['n_clientes'], label = "cities from RS")
# ax[0,1].set_xlabel("cities from SP")
# #ax[0,1].set_xticks(state_SP["customer_city"], state_SP["customer_city"], rotation=90, va='bottom')
# ax[0,2].set_xlabel("cities from RJ")
# ax[0,2].set_xticks(state_RJ["customer_city"], state_RJ["customer_city"], rotation=270, va='center')
# ax[1,0].set_xlabel("cities from MG")
# ax[1,0].set_xticks(state_MG["customer_city"], state_MG["customer_city"], rotation=270, va='top')
# ax[1,1].set_xlabel("cities from PR")
# ax[1,1].set_xticks(state_PR["customer_city"], state_PR["customer_city"], rotation=345, va='top')
# ax[1,2].set_xlabel("cities from RS")
# ax[1,2].set_xticks(state_RS["customer_city"], state_RS["customer_city"], rotation=345, va='top')
#plt.show()

# fig, ax = plt.subplots()
# ax.bar(state_SP["customer_city"], state_SP['n_clientes'], label = "cities from SP")
# ax.bar(state_RJ["customer_city"], state_RJ['n_clientes'], label = "cities from RJ")
# ax.bar(state_MG["customer_city"], state_MG['n_clientes'], label = "cities from MG")
# ax.bar(state_PR["customer_city"], state_PR['n_clientes'], label = "cities from PR")
# ax.bar(state_RS["customer_city"], state_RS['n_clientes'], label = "cities from RS")
# ax.set_xlabel("cities from RJ")
# ax.set_xticks(state_RJ["customer_city"], state_RJ["customer_city"], rotation=270, va='center')
# ax.set_xlabel("cities from MG")
# ax.set_xticks(state_MG["customer_city"], state_MG["customer_city"], rotation=270, va='top')
# ax.set_xlabel("cities from PR")
# ax.set_xticks(state_PR["customer_city"], state_PR["customer_city"], rotation=345, va='top')
# ax.set_xlabel("cities from RS")
# ax.set_xticks(state_RS["customer_city"], state_RS["customer_city"], rotation=345, va='top')
#plt.show()

#Grafico de dispersión
# colors = {"SP": "Crimson", "RJ":"RoyalBlue", "MG":"DarkSeaGreen", "PR":"Gold", "RS":"Plum"}
# color_states = tabla_estado_ciudad.customer_state.map(colors)
# fig, ax = plt.subplots()
# for state in set(tabla_estado_ciudad.customer_state):
#     ax.scatter(    
#         tabla_estado_ciudad.customer_state[tabla_estado_ciudad.customer_state== state], 
#         tabla_estado_ciudad.customer_city[tabla_estado_ciudad.customer_state == state],
#         s = tabla_estado_ciudad.n_clientes[tabla_estado_ciudad.customer_state == state],
#         c = colors[state])
#plt.show()

#2. Añadir dos columnas, numero de pedido y porcentaje respecto al total de pedidos
#Creamos df agrupado por customer_id y creamos una columna nueva que nos cuente la cantidad de pedidos (order_id)
pedidos_clientes = df_orders_customers_payments_items_review.groupby("customer_id").agg(count_orders = ("order_id", "count")).reset_index().sort_values(by="count_orders", ascending=False)
#Actualizamos df agrupando por numero de pedidos y agregando columna nueva con la cantidad de clientes que tiene el mismo número de pedidos.
pedidos_clientes = pedidos_clientes.groupby("count_orders").agg(count_customer=("customer_id", "count")).reset_index().sort_values(by="count_orders", ascending=False)
print(pedidos_clientes)
"""Barajamos posibilidades. ¿Mejor plot, pie o bigotes? Hay muchos clientes que han hecho muy pocos pedidos y muy pocos clientes que han hecho muchos pedidos"""
#plt.plot(pedidos_clientes["count_orders"], pedidos_clientes["count_customer"])
#plt.pie(pedidos_clientes["count_customer"])
#plt.boxplot(x=pedidos_clientes["count_customer"])
plt.hist(pedidos_clientes["count_orders"], bins=20)
plt.show()
#3. nº pedidos tarde por ciudad. % respecto al total de pedidos por ciudad. Tiempo medio de días tarde.


#4. nº de review por stado y score medio en cada una
"""Falta eliminar los reviews de los pedidos retrasados"""

#Creamos df agrupado por estado en el que añadimos dos columnas, nº de reviews por estado y su media. 
# No añadimos reset_index() porque nos interesa usar la columna de estado como indice en el gráfico. Ordenamos por media para mejor visualización
review_state = df_orders_customers_payments_items_review.groupby("customer_state").agg(
    num_review = ("review_id", "count"), mean_score = ("review_score", "mean")).reset_index().sort_values(by="mean_score", ascending=False)

#Creamos la figura
fig= plt.figure()
#Añadimos un subplot a la figura
ax1 = fig.add_subplot(111)
#twinx() sirve para tener dos ejes "y" en lugar de uno. Su "gemelo" en el lado contrario
ax2 = ax1.twinx()
#Creamos los gráficos, indicando como eje x, común a ambos, los estados. Como eje y, cada gráfico tiene el suyo.
ax1.bar(review_state["customer_state"],review_state["num_review"], color = "Teal")
ax2.plot(review_state["customer_state"],review_state["mean_score"], color="Maroon")

#Añadimos el límite en el eje y del gráfico de línea en 5, dado que se puntúa entre 0 y 5. 
ax2.set_ylim(0,  5)
#Indicamos que el eje y del gráfico de reviews va a la derecha
ax1.yaxis.set_label_position("right")
#Movemos las etiquetas del eje al lado derecho.
ax1.yaxis.tick_right()
#Indicamos que el eje y del gráfico de medias va a la izquierda
ax2.yaxis.set_label_position("left")
#Dejamos las etiquetas del eje en el lado izquierdo
ax2.yaxis.tick_left()
#Con spines podemos mostrar u ocultar los ejes del gráfico. En este caso, ocultamos (set_visible(False)) el eje superior ("top")para simplificar el gráfico
ax1.spines['top'].set_visible(False)
ax2.spines['top'].set_visible(False)

#Añadimos título y etiquetas a los ejes
fig.suptitle("Mean and number of reviews in each State")
ax2.set_ylabel("Mean Score")
ax1.set_ylabel("Number of reviews")
ax1.set_xlabel("States")

