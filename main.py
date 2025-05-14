import pandas as pd
import os
import matplotlib.pyplot as plt
import numpy as np
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
df_ejercicio4.to_csv('df_ejercicio4.csv', index=False)


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
pedidos_clientes = df_orders_customers_payments_items_review.groupby(["customer_state", "customer_city"]).agg(
    count_orders = ("order_id", "count"), count_customers = ("customer_id", "count")
    ).reset_index().sort_values(by="count_orders", ascending=False)
print(pedidos_clientes)
fig_pedidos_clientes_estado, ax = plt.subplots()
ax.bar(pedidos_clientes["customer_state"], pedidos_clientes["count_customers"])
#fig_pedidos_clientes_estado("Pedidos realizados por estado")
fig_boxplot_clientes, ax1 = plt.subplots()
ax1.boxplot(x=pedidos_clientes["count_customers"])
fi_dispersion_pedidos_clientes, ax2= plt.subplots()
ax2.scatter(pedidos_clientes["count_customers"], pedidos_clientes["count_orders"])

#3. nº pedidos tarde por ciudad. % respecto al total de pedidos por ciudad. Tiempo medio de días tarde.
#Creamos df agrupando ciudades y creando dos columnas, una con la cantidad de pedidos retrasados y otra con el número de pedidos por ciudad.

pedidos_tarde =df_orders_customers_payments_items_review.groupby("customer_city").agg(
     cantidad_pedidos_tarde = ("dias_retraso_entrega", lambda x: (x > 0).sum()),
     media_dias_tarde = ("dias_retraso_entrega", lambda x: x[x > 0].mean()),
     num_pedidos =("order_id", "count")).reset_index().sort_values(by="cantidad_pedidos_tarde", ascending=False).head(20)
#Añadimos una columna con el porcentaje de pedidos tarde.
pedidos_tarde["porcentaje_tarde"] = round(pedidos_tarde.cantidad_pedidos_tarde/pedidos_tarde.num_pedidos*100, 2)
print(pedidos_tarde)

#Creamos la figura
fig_pedidos_retraso_barras, ax3= plt.subplots()

#Creamos ambas barras, apiladas
retrasos = ax3.bar(pedidos_tarde["customer_city"], pedidos_tarde["cantidad_pedidos_tarde"], label = "cantidad_pedidos_tarde", facecolor = 'mediumseagreen' )
pedidos = ax3.bar(pedidos_tarde["customer_city"], pedidos_tarde["num_pedidos"], bottom=pedidos_tarde["cantidad_pedidos_tarde"], label = "num_pedidos", facecolor= 'xkcd:sky blue')

#Añadimos titulo y etiquetas
# ax3.legend()
# ax3.bar_label(retrasos, padding=0)
# ax3.bar_label(pedidos, padding=3)
ax3.set_xticks(pedidos_tarde["customer_city"],pedidos_tarde["customer_city"], rotation = "vertical")

#fig_pedidos_retraso_barras.suptitle("Relación de retrasos y pedidos totales por ciudad")


fig_media_retraso_ciudad, ax4 = plt.subplots()
ax4.barh(pedidos_tarde["customer_city"], pedidos_tarde["media_dias_tarde"])

#4. nº de review por stado y score medio en cada una

#Creamos df agrupado por estado en el que añadimos dos columnas, nº de reviews por estado y su media. 
# No añadimos reset_index() porque nosorders_customers_payments_items_review interesa usar la columna de estado como indice en el gráfico. Ordenamos por media para mejor visualización
review_state = df_ejercicio4.groupby("customer_state").agg(
    num_review = ("review_id", "count"), mean_score = ("review_score", "mean")).reset_index().sort_values(by="mean_score", ascending=False)

#Creamos la figura
fig_num_review_media= plt.figure()
#Añadimos un subplot a la figura
ax5 = fig_num_review_media.add_subplot(111)
#twinx() sirve para tener dos ejes "y" en lugar de uno. Su "gemelo" en el lado contrario
ax6 = ax5.twinx()
#Creamos los gráficos, indicando como eje x, común a ambos, los estados. Como eje y, cada gráfico tiene el suyo.
ax5.bar(review_state["customer_state"],review_state["num_review"], color = "Teal")
ax6.plot(review_state["customer_state"],review_state["mean_score"], color="Maroon")

#Añadimos el límite en el eje y del gráfico de línea en 5, dado que se puntúa entre 0 y 5. 
ax6.set_ylim(0,  5)
#Indicamos que el eje y del gráfico de reviews va a la derecha
ax5.yaxis.set_label_position("right")
#Movemos las etiquetas del eje al lado derecho.
ax5.yaxis.tick_right()
#Indicamos que el eje y del gráfico de medias va a la izquierda
ax6.yaxis.set_label_position("left")
#Dejamos las etiquetas del eje en el lado izquierdo
ax6.yaxis.tick_left()
#Con spines podemos mostrar u ocultar los ejes del gráfico. En este caso, ocultamos (set_visible(False)) el eje superior ("top")para simplificar el gráfico
ax5.spines['top'].set_visible(False)
ax6.spines['top'].set_visible(False)

#Añadimos título y etiquetas a los ejes
# fig_num_review_media.suptitle("Mean and number of reviews in each State")
# ax6.set_ylabel("Mean Score")
# ax5.set_ylabel("Number of reviews")
# ax5.set_xlabel("States")

#plt.show()

solo_retrasados = df_orders_customers_payments_items_review[df_orders_customers_payments_items_review["dias_retraso_entrega"]>0]
solo_retrasados["approved-purchase"] = (solo_retrasados["order_approved_at"]-solo_retrasados["order_purchase_timestamp"]).dt.days
solo_retrasados["delivered_carrier-approved"] = (solo_retrasados["order_delivered_carrier_date"]- solo_retrasados["order_approved_at"]).dt.days
solo_retrasados["estimated_delivery-delivered_carrier"] = (solo_retrasados["order_estimated_delivery_date"] - solo_retrasados["order_delivered_carrier_date"]).dt.days
solo_retrasados["delivered_customer-estimated_delivery"] = (solo_retrasados["order_delivered_customer_date"] - solo_retrasados["order_estimated_delivery_date"]).dt.days
print(solo_retrasados)

print(solo_retrasados["approved-purchase"].mean(), solo_retrasados["approved-purchase"].std())
print(solo_retrasados["delivered_carrier-approved"].mean(), solo_retrasados["delivered_carrier-approved"].std())
print(solo_retrasados["estimated_delivery-delivered_carrier"].mean(), solo_retrasados["estimated_delivery-delivered_carrier"].std())
print(solo_retrasados["delivered_customer-estimated_delivery"].mean(), solo_retrasados["delivered_customer-estimated_delivery"].std())

fig_diferencia_aprobado_compra, ax7 = plt.subplots()
# #Hay algunos outliers 
ax7.boxplot( solo_retrasados["approved-purchase"])
fig_diferencia_almacen_aprobado, ax8 = plt.subplots()
ax8.boxplot(solo_retrasados["delivered_carrier-approved"])
fig_diferencia_entrega_estimada_almacen, ax9 = plt.subplots()
ax9.boxplot( solo_retrasados["estimated_delivery-delivered_carrier"])
#Claramente se ve un problema en la estimación de la fecha de entrega de los pedidos.
fig_diferencia_entrega_estimacion, ax10 = plt.subplots() 
ax10.boxplot( solo_retrasados["delivered_customer-estimated_delivery"])
#plt.show()


error_estimacion_fecha = solo_retrasados.groupby("delivered_customer-estimated_delivery")["order_id"].count().reset_index().sort_values(by="order_id", ascending=False)
print(error_estimacion_fecha)
fig_error_estimacion_fecha, ax11 = plt.subplots()
ax11.scatter( error_estimacion_fecha["order_id"], error_estimacion_fecha["delivered_customer-estimated_delivery"])
plt.show()

#relacion_review_precio = df_orders_customers_payments_items_review["customer_id", ""]
print(df_ejercicio4.head(30))