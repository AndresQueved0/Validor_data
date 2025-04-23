# Databricks notebook source
from pyspark.sql import SparkSession
from datetime import datetime, timedelta, date
from pyspark.errors import PySparkException
from pyspark.sql.functions import date_format, col
import json
import os
import os
# from src.utilities.control_sql import ControlSQL
import uuid
import sys
sys.path.append('/Workspace/Repos/src')
from migracion_teradata_sfc.src.utilities.control_sql import ControlSQL

p_proceso = dbutils.widgets.get("p_proceso")
p_TipEnt =  int(dbutils.widgets.get("p_TipEnt"))
p_CodEnt =  int(dbutils.widgets.get("p_CodEnt"))
p_CodFor =  int(dbutils.widgets.get("p_CodFor"))
p_FecBal =  dbutils.widgets.get("p_FecBal")
concurrency = dbutils.widgets.get("concurrency").lower() == "true"
p_TipEnt_Hom = None
p_FlagHom = False

try:
    p_TipEnt_Hom = int(dbutils.widgets.get("p_TipEnt_Hom"))
    p_FlagHom = bool(dbutils.widgets.get("p_FlagHom"))
except:
    p_TipEnt_Hom = None
    p_FlagHom = False

environment = os.getenv('DBWS_ENV')
location = os.getenv('LOCATION_ADLS')
location_sz  = location.format(container="silver-zone")
logs = []
state = True

# p_proceso = "CSCosech"
# p_TipEnt =  1
# p_CodEnt =  42 #42
# p_CodFor = 453
# p_FecBal =  date(2024,9,30)

SP_NAME = "CSCosech"
ProcessCD = '1005'
V_Status = "Running"
V_PRUGC = "PROD_DWH_COSECHAS_CONF"

PATH_LOG = "/Workspace/Repos/src/modelo_cosechas/Transvesales/Sp_Load_Log_Model"

def current_timestamp():
    return datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    
def print_log(str_log):
    print(f"{current_timestamp()} - {str_log}")
    logs.append(f"{current_timestamp()} - {str_log}")

try:
    timestamp = (current_timestamp())
    tip_ent = p_TipEnt_Hom if p_FlagHom else p_TipEnt
    job_run_id = str(uuid.uuid4())
    if environment == "dev":
        nb_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())["tags"]
        job_run_id = str(nb_info["runId"])
    retries =  15
    retry = 0
    sleep_retry = 60
    control_sql = ControlSQL(spark, SP_NAME, 'csc_cosechassaldos', job_run_id, retries, sleep_retry, 1000, str(p_CodEnt))

    # ---------------------------------CASTEO DE COLUMNA A NUMERO PARA PODER HACER EL PIVOTEO
    print_log("Ingreso a cestear columnas a tipo numerico")
    col_format=spark.sql(f"""show columns from delta.`{location_sz}/Accepted/formato{p_CodFor}`""")

    cols = col_format.filter(col("col_name").like("COL%")).collect()
    str_cols = ""
    for col in cols:
        str_cols += f"{col['col_name']} as `{col['col_name'][3:]}`, "
    cols_pivot = str_cols[:-2]

    logs.append(f"{current_timestamp()} Cols: {str_cols}")

    schema_table = spark.sql(f"select * FROM delta.`{location_sz}/Accepted/formato{p_CodFor}/Fecha_Corte={p_FecBal}/Tipo_Entidad={tip_ent}/Codigo_Entidad={p_CodEnt}`")

    casted_columns = ''

    for columna in schema_table.schema:
        casted_columns += columna.name + ',' if 'COL' not in columna.name else f"cast({columna.name} as DOUBLE),"

    schema_table.createOrReplaceTempView('df_casteado')

    query = spark.sql(f"select {casted_columns[:-1]} from df_casteado")
    query.createOrReplaceTempView("df_query")

    print_log("Salio de cestear columnas a tipo numerico")
    print_log("Ingreso a crear lote de ejecucion")

    cidt = spark.sql(f"select distinct cidt from df_query").first()[0]

    logs.append(f"{current_timestamp()} Cidt: {cidt}")

    # -----------LOGICA PARA DEJAR CON EL MISMO LOTE LOS REGISTROS DE LOS FROMATOS YA QUE CORRESPONDEN AL MISMO PLANO
    df = spark.sql(f"""   
            SELECT 
               sk_lote
            FROM prod_dwh_cosechas_conf.sk_lote 
            WHERE 
                N_proceso = 8 
                AND Proceso = '{SP_NAME}' 
                AND Fecha_Corte = '{p_FecBal}' 
                AND Tipo_Entidad = {tip_ent} 
                AND Codigo_Entidad = {p_CodEnt}
                AND Cidt = '{cidt}'
        """)
    
    if df.count() > 0: # Si existe se toma el lote actual
        SK_lote = df.first()[0]
    else:
        SK_lote = spark.sql(f"""select CAST(UNIX_TIMESTAMP() AS INT)""").first()[0]
        spark.sql(f""" 
            INSERT INTO prod_dwh_cosechas_conf.sk_lote  
            SELECT 
               8 AS N_proceso,
                '{p_proceso}' AS Proceso,
                '{p_FecBal}' AS Fecha_Corte,
                {tip_ent} AS Tipo_Entidad,
                {p_CodEnt} AS Codigo_Entidad,
                '{cidt}' AS Cidt,
                CAST({SK_lote} AS BIGINT)AS Sk_Lote,
                from_utc_timestamp(CAST(current_timestamp() AS TIMESTAMP), 'America/Bogota') AS Fecha_Ejecucion 
        """)

    print_log(f"Lote de ejecucion {SK_lote}")
    print_log("Salio de crear lote de ejecucion")

    #--------------------------------INSERT DE EJECUCION EN LA TABLA LOGTD
    dbutils.notebook.run(PATH_LOG, 0, {"V_Lote":SK_lote, "V_NumeroProceso":ProcessCD, "V_NameProcess": SP_NAME, "V_Status":V_Status, "V_PRUGC":V_PRUGC})
    
    print_log("Ingreso a eliminar registros existentes en CSC_FORMATOCOSECHAS")
    CS_SQL_Text = f"""
        DELETE FROM PROD_DWH_COSECHAS_DATA.CSC_FORMATOCOSECHAS CSC
        WHERE EXISTS (
            SELECT 1
            FROM PROD_DWH_COSECHAS_DATA.CSC_FORMATOCOSECHAS CSC_SUB
            INNER JOIN PROD_DWH_COSECHAS_DATA.FORMATO F
                ON F.FOR_ID = CSC_SUB.FCSC_FOR_ID
            INNER JOIN PROD_DWH_COSECHAS_DATA.ENTIDADES ENT
                ON ENT.ENT_ID = CSC_SUB.FCSC_ENT_ID
            WHERE F.FOR_CODIGO = '{p_CodFor}'
            AND CSC_SUB.FCSC_FECHA_BALANCE = '{p_FecBal}'
            AND ENT.ENT_TIPO = {tip_ent}
            AND ENT.ENT_ENTIDAD = {p_CodEnt}
            AND CSC_SUB.FCSC_FOR_ID = CSC.FCSC_FOR_ID
            AND CSC_SUB.FCSC_ENT_ID = CSC.FCSC_ENT_ID
            AND CSC_SUB.FCSC_FECHA_BALANCE = CSC.FCSC_FECHA_BALANCE
        )
    """

    if concurrency:
      control_sql.sql(CS_SQL_Text)
    else:
      spark.sql(CS_SQL_Text)
    print_log("Salio de eliminar registros existentes en CSC_FORMATOCOSECHAS")

    print_log(f"Ingreso a insertar datos a la tabla para el formato: {p_CodFor}")
    CS_SQL_Text = (f"""
            with prueba as (
                select
                    Load_Process_Id,
                    tipo_entidad,
                    codigo_entidad,
                    fecha_corte,
                    codigo_formato,
                    cast(Nivel2 as int) as numero_columna,
                    cod_unidad_captura,
                    cod_subcuenta,
                    '+' as signo,
                    columna_informacion as valor,
                    NULL as indicador,
                    codigo_formato,
                    Consecutivo
                FROM
                df_query as fmto UNPIVOT (
                    columna_informacion FOR Nivel2 IN (
                        {cols_pivot}
                    )
                )
            )
            INSERT INTO PROD_DWH_COSECHAS_DATA.CSC_FORMATOCOSECHAS
                select 
                    F.FOR_ID AS FCSC_FOR_ID,
                    ENT.ENT_ID AS FCSC_ENT_ID,
                    CFO.COL_ID AS FCSC_COL_ID,
                    UCF.UCA_ID AS FCSC_UCA_ID,
                    RGF.RGL_ID AS FCSC_RGL_ID,
                    TO_DATE(prueba.Fecha_Corte) AS FCSC_FECHA_BALANCE,
                    -- Los valores grandes se dejan en la columna valor como double y los pequeños se pasan a la columna numero como int
                    CASE WHEN TDF.TIPDAT_NOMBRE = 'VALOR' THEN CAST(prueba.valor AS DOUBLE) END AS FCSC_COLUMNA_VALOR,
                    CASE WHEN TDF.TIPDAT_NOMBRE = 'NUMERO' THEN CAST(prueba.valor AS INT) END AS FCSC_COLUMNA_NUMERO,
                    -- Se ajusta validación para tomar los campos fecha que son reportados en 0 por Null
                    CASE WHEN TDF.TIPDAT_NOMBRE = 'FECHA' AND CAST(prueba.valor AS INT) <> 0 THEN TO_DATE(
                        CONCAT(
                                SUBSTRING(LPAD(CAST(CAST(FEC.COL1 AS INT) AS STRING), 8, '0'), 5, 4), '-', 
                                SUBSTRING(LPAD(CAST(CAST(FEC.COL1 AS INT) AS STRING), 8, '0'), 3, 2), '-',
                                SUBSTRING(LPAD(CAST(CAST(FEC.COL1 AS INT) AS STRING), 8, '0'), 1, 2)
                            ), 
                        'yyyy-MM-dd')
                     ELSE NULL END AS FCSC_COLUMNA_FECHA,
                    CASE WHEN TDF.TIPDAT_NOMBRE = 'DESCRIPCION' THEN CAST(prueba.valor AS VARCHAR(250)) END AS FCSC_COLUMNA_DESC,
                    {SK_lote} AS Sk_Lote, 
                    NULL Sk_Lote_Upd,
                    1 AS Cod_Severidad 
                from prueba 
                LEFT JOIN PROD_DWH_COSECHAS_DATA.FORMATO F
                    ON prueba.Codigo_Formato = F.FOR_CODIGO
                    AND prueba.Fecha_Corte BETWEEN F.FOR_FECHA_DESDE AND F.FOR_FECHA_HASTA
                LEFT JOIN PROD_DWH_COSECHAS_DATA.ENTIDADES ENT
                    ON ENT.ENT_TIPO = prueba.Tipo_Entidad AND ENT.ENT_ENTIDAD = prueba.Codigo_entidad 
                LEFT JOIN PROD_DWH_COSECHAS_DATA.COLUMNAFORMATO CFO
                    ON CFO.COL_FOR_ID = F.FOR_ID AND CFO.COL_CODIGO = prueba.numero_columna 
                    AND prueba.Fecha_Corte BETWEEN CFO.COL_FECHA_DESDE AND CFO.COL_FECHA_HASTA
                LEFT JOIN PROD_DWH_COSECHAS_DATA.UNCAPTURAFORMATO UCF
                    ON UCF.UCA_FOR_ID = F.FOR_ID AND UCF.UCA_CODIGO = prueba.cod_unidad_captura
                    AND prueba.Fecha_Corte BETWEEN UCF.UCA_FECHA_DESDE AND UCF.UCA_FECHA_HASTA
                LEFT JOIN PROD_DWH_COSECHAS_DATA.RENGLONFORMATO RGF
                    ON F.FOR_ID = RGF.RGL_FOR_ID AND UCF.UCA_ID = RGF.RGL_UCA_ID AND prueba.cod_subcuenta = RGF.RGL_CODIGO
                    AND prueba.Fecha_Corte BETWEEN RGF.RGL_FECHA_DESDE AND RGF.RGL_FECHA_HASTA
                LEFT JOIN PROD_DWH_COSECHAS_DATA.REL_UNCAPTURA_COLUMNAFORMATO RUCF
                    ON RUCF.RELUCACOL_UCA_ID = UCF.UCA_ID AND RUCF.RELUCACOL_COL_ID = CFO.COL_ID
                    AND prueba.Fecha_Corte BETWEEN RUCF.RELUCACOL_FECHA_DESDE AND RUCF.RELUCACOL_FECHA_HASTA
                    --Join Estructura TIPODATOFORMATO para hacer logica en las columnas de valores y fechas
                LEFT JOIN PROD_DWH_COSECHAS_DATA.TIPODATOFORMATO TDF
                    ON TDF.TIPDAT_ID = RUCF.RELUCACOL_TIPDAT_ID
                LEFT JOIN df_query AS FEC
                    ON prueba.Cod_Unidad_Captura = FEC.Cod_Unidad_Captura AND prueba.Cod_SubCuenta = FEC.Cod_SubCuenta
                    AND prueba.Consecutivo = FEC.Consecutivo
                WHERE F.FOR_CODIGO = {p_CodFor}
            """)

    if concurrency:
      control_sql.sql(CS_SQL_Text)
    else:
      spark.sql(CS_SQL_Text)
    print_log(f"Salio de insertar datos a la tabla para el formato: {p_CodFor}")
    print_log(f"Termina acualizacion de CSC_FORMATOCOSECHAS para el formato: {p_CodFor}")

    #--------------------------------UPDATE ESTADO A SATISFACTORIO DURANTE LA EJECUCION
    V_Status = "Successful"
    dbutils.notebook.run(PATH_LOG, 0, {"V_Lote":SK_lote, "V_NumeroProceso":ProcessCD, "V_NameProcess": SP_NAME, "V_Status":V_Status, "V_PRUGC":V_PRUGC})

    dbutils.notebook.exit(json.dumps({"state": state, "logs": logs}))
except PySparkException as e:
    state = False
    MySQLCode = e.getSqlState()
    if MySQLCode is None:
        MySQLCode = "HY000"
    SQLmsg = e.getErrorClass() 
    print_log(e)
    logs.append(f"{current_timestamp()} Error: {e}")
    #--------------------------------UPDATE ESTADO A ERROR DURANTE LA EJECUCION
    V_Status = "Failed"
    dbutils.notebook.run(PATH_LOG, 0, {"V_Lote":SK_lote, "V_NumeroProceso":ProcessCD, "V_NameProcess": SP_NAME, "V_Status":V_Status, "V_PRUGC":V_PRUGC})
    qryUpdLog = f"""
	        UPDATE {V_PRUGC}.LogTd SET CodeSQL = '{MySQLCode}' WHERE Lote = {SK_lote}
    """
    dbutils.notebook.exit(json.dumps({"state": state, "logs": logs}))