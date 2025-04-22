# Databricks notebook source
# DBTITLE 1,Validador Data Teradata
from pyspark.sql.functions import col, cast, upper, when, lit,current_date, date_format,coalesce,abs
from pyspark.sql.types import *
import json
from functools import reduce
from datetime import datetime,timedelta

# COMMAND ----------

dbutils.widgets.text("id_parametro", "","")
dbutils.widgets.text("fecha_corte", "","")
dbutils.widgets.text("operador", "","")
dbutils.widgets.text("origen", "","")

id_parametro = dbutils.widgets.get("id_parametro")
fecha_corte = dbutils.widgets.get("fecha_corte")
operador = dbutils.widgets.get("operador")
fecha_corte = datetime.strptime(fecha_corte, "%Y-%m-%d").date()
fecha_validacion = (datetime.now() - timedelta(hours=5)).date()
origen = dbutils.widgets.get("origen")

print(f"id_parametro: {id_parametro}")
print(f"fecha_corte: {fecha_corte}")
print(f"operador: {operador}")
print(f"fecha_validacion: {fecha_validacion}")
print(f"origen: {origen}")

# COMMAND ----------

# DBTITLE 1,Lectura parámetros config.json
with open("./config.json", "r") as file:
    config = json.load(file)
    
query_metadatos = config['query_metadatos']
query_metadatos = query_metadatos.replace("{id_parametro}", id_parametro)
partition_column_result = config['partition_column_result']
if origen == "teradata":
    table_path_result = config['table_path_result_teradata'] 
    path_read = config['directorio_read_teradata']
else :
    table_path_result = config['table_path_result_oracle'] 
    path_read = config['directorio_read_oracle']

print(f"query_metadatos:{query_metadatos}")
print(f"table_path_result:{table_path_result}")
print(f"path_read:{path_read}")

# COMMAND ----------

# DBTITLE 1,Consulta Metadatos
metadatos = spark.sql(query_metadatos)
query_azure = metadatos.select(col("query_azure")).first()[0]
formatos = metadatos.select(col("formatos")).first()[0]
informe = metadatos.select(col("informe")).first()[0]

columnas_comparar = metadatos.select(col("alias_comparar")).first()[0].upper()
#columnas_comparar = metadatos.select(col("columnas_comparar")).first()[0].upper()
columnas_comparar = columnas_comparar.split(',')

metadatos.display()

# COMMAND ----------

# DBTITLE 1,Lectura csv de la fuente
#Se realiza la lectura del archivo csv generado por la consulta en Data Factory
import os

secret_scope = os.getenv('SS_NAME')

connection_config = json.loads(dbutils.secrets.get(scope=secret_scope, key="akv-secret-ingest"))

# connection_config = json.loads(dbutils.secrets.get(
#             scope="kv-ss-dev-teradata", key="akv-secret-ingest"))

adls_key = connection_config['adls']['adls_key']
adls_name = connection_config['adls']['adls_name']
spark.conf.set(f"fs.azure.account.key.{adls_name}.dfs.core.windows.net",adls_key)

contenedor_read = "raw-test"
directorio_read = path_read.replace("{id_parametro}", id_parametro)
print(f"directorio_read:{directorio_read}")

adlsPath = f'abfss://{contenedor_read}@{adls_name}.dfs.core.windows.net/'
dataframe = spark.read.format("csv").option("header", True).option("sep", ",").option('multiLine', True).option("inferSchema" , "true").load(adlsPath+directorio_read)

columna_fecha = dataframe.columns[0]
print(f"Columna fecha_corte = {columna_fecha}")
dataframe.printSchema()
for columna, tipo in dataframe.dtypes:
    if tipo == 'double':
        dataframe = dataframe.withColumn(columna, col(columna).cast("decimal(29,2)"))

dataframe = dataframe.withColumn(columna_fecha, col(columna_fecha).cast("date"))
dataframe = dataframe.withColumnRenamed(columna_fecha, "FECHA_CORTE")

# dataframe.printSchema()
# dataframe.display()


# COMMAND ----------

# DBTITLE 1,Escribe datos en tabla delta
#Funcion que crea o escribe en la tabla Delta 
def get_delta_table (df,delta_table_path,partition_date,partition_column,id_parametro,fecha_valdiacion) :
    print(f"Escribiendo tabla...")
    print(f"fecha de particion: {partition_date}")
    print(f"fecha_valdiacion:{fecha_valdiacion}")
    print(f"Id de parametro: {id_parametro}")
    table_exists = spark.catalog.tableExists(delta_table_path)
    
    # if not table_exists:

    #     df.write\
    #     .partitionBy(partition_column,"ID_PARAMETRO")\
    #     .format("delta")\
    #     .mode("overwrite")\
    #     .saveAsTable(delta_table_path)
    # else:
    #     df.write\
    #     .format("delta")\
    #     .mode("overwrite")\
    #     .option("replaceWhere", f"FECHA_VALIDACION ='{fecha_validacion}' AND {partition_column} = '{partition_date}' AND id_parametro = {id_parametro}")\
    #     .saveAsTable(delta_table_path)


    if not table_exists:
        df.write\
            .partitionBy("FECHA_VALIDACION", partition_column, "ID_PARAMETRO")\
            .format("delta")\
            .mode("overwrite")\
            .saveAsTable(delta_table_path)
    else:
        # Verifica si la partición existe antes de usar replaceWhere
        partition_exists = spark.sql(f"SELECT COUNT(1) FROM {delta_table_path} WHERE FECHA_VALIDACION = '{fecha_validacion}' AND {partition_column} = '{partition_date}' AND ID_PARAMETRO = {id_parametro}").collect()[0][0] > 0

        if partition_exists:
            df.write\
                .format("delta")\
                .mode("overwrite")\
                .option("replaceWhere", f"FECHA_VALIDACION = '{fecha_validacion}' AND {partition_column} = '{partition_date}' AND ID_PARAMETRO = {id_parametro}")\
                .saveAsTable(delta_table_path)
        else:
            df.write\
                .partitionBy("FECHA_VALIDACION", partition_column, "ID_PARAMETRO")\
                .format("delta")\
                .mode("append")\
                .saveAsTable(delta_table_path)


# COMMAND ----------

# DBTITLE 1,Ejecuta  Querys en Azure
#Remplaza valores variables en la Query de Azure
query_azure = query_azure.replace("{formatos}", formatos)
query_azure = query_azure.replace("{informe}", informe)
query_data_azure = query_azure.replace("{fecha_corte}", fecha_corte.strftime("%Y-%m-%d"))
query_data_azure = query_data_azure.replace("{operador}", operador)
print(f"query_data_azure = {query_data_azure}")

#Ejecuta la Query 
sql_data_az = spark.sql(query_data_azure)
#sql_data_or = spark.sql(query_data_origen)
sql_data_or = dataframe
sql_data_az = sql_data_az
# sql_data_or.printSchema()
# sql_data_az.printSchema()

# COMMAND ----------

# DBTITLE 1,Renombra las columnas
#Renombre la columas del df origen y del df azure para las comparaciones
def rename_column(df,azure,columnas_comparar) :
    columnas = df.columns
    columnas_operaciones = []
    nuevos_nombres_columnas = {}
    contador = 1

    for columna in columnas:
        if columna.upper() == "TIE_FECHA" or columna.upper() == "FECHA_CORTE" :
            if azure == True :
                nuevos_nombres_columnas[columna] = "FECHA_CORTE_AZ"
            else: 
                nuevos_nombres_columnas[columna] = "FECHA_CORTE"
        else:
            if azure == True :
                nuevos_nombres_columnas[columna] = f"VALOR{contador}_AZ"
                contador += 1
            else :
                nuevos_nombres_columnas[columna] = f"VALOR{contador}"
                contador += 1

    for columna_original, columna_nueva in nuevos_nombres_columnas.items():
        print(f"{columna_original}-{columna_nueva}")
        df = df.withColumnRenamed(columna_original, columna_nueva)
    if azure == True :
        nuevos_nombres_columnas = {key.upper(): value for key, value in nuevos_nombres_columnas.items()}

        for valor in nuevos_nombres_columnas:
            if valor in columnas_comparar:
                columnas_operaciones.append((nuevos_nombres_columnas[valor]))
    
    for column in columnas_operaciones:
        df = df.withColumn(column, col(column).cast(DecimalType(29, 2)))

    #df.printSchema()
    return df,columnas_operaciones


sql_data_or,columnas_operaciones = rename_column(sql_data_or,False,columnas_comparar)
#sql_data_or.printSchema()
sql_data_az,columnas_operaciones = rename_column(sql_data_az,True,columnas_comparar)
# sql_data_az.printSchema()
# print(columnas_operaciones)

# COMMAND ----------

# sql_data_or.display()
# sql_data_az.display()

# COMMAND ----------

# DBTITLE 1,Unión de df
#Se realiza la union de los df por todas las columnas exepto las de operaciones (suma,coteo,promedio,etc)
df2 = sql_data_az
df1 = sql_data_or
print(f"columnas_operaciones:{columnas_operaciones}")
# Lista de columnas a excluir en la llaves de la union
columnas_operaciones_az = columnas_operaciones
columnas_operaciones_or = [col.replace('_AZ', '') for col in columnas_operaciones]
print(f"columnas az: {columnas_operaciones_az}")
print(f"columnas or: {columnas_operaciones_or}")
# Filtrar columnas de df1 y df2 excluyendo las especificadas
cols_df12 = [col for col in df1.columns if col not in columnas_operaciones_or]
cols_df22 = [col for col in df2.columns if col not in columnas_operaciones_az]
print(cols_df12)
print(cols_df22)

# Asegurarse de que ambas listas tengan la misma longitud
assert len(cols_df12) == len(cols_df22), "Las listas de columnas no coinciden en longitud"

# Generar las condiciones de unión
df_joined = [
    df1[col1] == df2[col2] 
    for col1, col2 in zip(cols_df12, cols_df22)
]

# Realizar la unión
df_joined_result = df1.join(df2, df_joined, how="outer")



# COMMAND ----------

#df_joined_result.display()

# COMMAND ----------

# DBTITLE 1,Realiza comparaciones
#se realizan las comparaciones por todas las columnas
#si coinciden asiga estado correcto
#si no coinciden asiga estado inconsistencia
tolerancia = 0.10 #margen de diferencia para las columnas de operaciones
columnas = [col for col in df1.columns]
select_exprs = []
comparacion = []

for col_name in columnas:
    #si fecha_corte es vacia asigna el valor de fecha_corte_az
    if col_name == "FECHA_CORTE":
        select_exprs.append(
            when(
                col(col_name).isNull() | (col(col_name) == lit("")),
                col(f"{col_name}_AZ")
            ).otherwise(col(col_name)).alias(col_name)
        )
    else:
        select_exprs.append(coalesce(col(col_name), lit("")).alias(col_name))

    select_exprs.append(coalesce(col(f"{col_name}_AZ"), lit("")).alias(f"{col_name}_AZ"))

for col_name in columnas:
    if col_name in columnas_operaciones_or:
        comparacion.append(abs(coalesce(col(col_name), lit(0)) - coalesce(col(f"{col_name}_AZ"), lit(0))) <= tolerancia)
    else:
        comparacion.append(
            coalesce(col(col_name)) == coalesce(col(f"{col_name}_AZ"))
        )

faltante_conditions = []
sobrante_conditions = []


for col_name in columnas:
    if col_name != "FECHA_CORTE": 
        faltante_conditions.append(
            col(f"{col_name}_AZ").isNull() | (col(f"{col_name}_AZ") == lit(""))
        )

faltante_expr = reduce(lambda x, y: x & y, faltante_conditions)

for col_name in columnas:
    if col_name != "FECHA_CORTE":  
        faltante_conditions.append(
            (col(col_name).isNotNull() & (col(f"{col_name}_AZ").isNull() | (col(f"{col_name}_AZ") == lit(""))))
        )
        sobrante_conditions.append(
            (col(f"{col_name}_AZ").isNotNull() & (col(col_name).isNull() | (col(col_name) == lit(""))))
        )

sobrante_expr = reduce(lambda x, y: x | y, sobrante_conditions)


estado_expr = when(
    reduce(lambda x, y: x & y, comparacion),
    lit("correcto")
).otherwise(
    when(
        faltante_expr,
        lit("faltante")
    ).otherwise(
        when(
            sobrante_expr,
            lit("sobrante")
        ).otherwise(lit("inconsistencia"))
    )
).alias("estado")

select_exprs.append(estado_expr)

df_result = df_joined_result.select(*select_exprs)
#se agregan columnas de control fecha_validacion y id_parametro
df_result = df_result.withColumn("FECHA_VALIDACION", lit(fecha_validacion))
df_result = df_result.withColumn("ID_PARAMETRO", lit(id_parametro).cast(IntegerType()))

# COMMAND ----------

df_final = df_result.withColumn(
    "VALOR2",
    when(df_result["estado"]=="sobrante",df_result["VALOR2_AZ"]).otherwise(df_result["VALOR2"])
)

# COMMAND ----------

# df_result1 = df_result.filter(col("estado") == "sobrante")
# df_result1.display()

# COMMAND ----------

# df_result2 = df_result.withColumn(
#     "VALOR2",
#     when(df_result["estado"]=="sobrante",df_result["VALOR2_AZ"]).otherwise(df_result["VALOR2"])
# )
# df_result2.display()

# COMMAND ----------

# DBTITLE 1,Escribe resultado final
try:
    fechas_corte = df_final.select("FECHA_CORTE").distinct().collect()
    lista_fechas = [row.FECHA_CORTE.strftime("%Y-%m-%d") for row in fechas_corte]

    if not lista_fechas:  # Verifica si la lista está vacía
        raise ValueError("No se encontraron fechas en 'FECHA_CORTE'. Verificar el DataFrame.")

    print(f"Fechas de corte a escribir: {lista_fechas}")
    count_fechas = len(lista_fechas)

    if count_fechas > 1:
        for fila in fechas_corte:
            fecha_corte = fila['FECHA_CORTE']
            df_filtered = df_final.filter(
                (col("FECHA_CORTE") == fecha_corte) & (col("ID_PARAMETRO") == id_parametro)
            )      
            get_delta_table(df_filtered, table_path_result, fecha_corte, partition_column_result, id_parametro, fecha_validacion)
    else:
        fecha_corte = lista_fechas[0]
        get_delta_table(df_final, table_path_result, fecha_corte, partition_column_result, id_parametro, fecha_validacion)

except Exception as e:
    print(f"Error: {e}")

