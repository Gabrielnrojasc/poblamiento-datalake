-- COMANDO DE EJECUCION
-- beeline -u jdbc:hive2:// -f /home/gabrielrc/procesos/Poblando_Capa_Functional.sql --hiveconf "PARAM_USERNAME=gabrielrc" --hiveconf "ENV=DEV" 

-- -------------------------------------------------------------------------------------------------------
-- 
-- @section 1. Definici�n de par�metros
-- 
-- -------------------------------------------------------------------------------------------------------

-- [HIVE] Creamos una variable en HIVE
SET ENV=dev;
SET PARAM_USERNAME=gabrielrc;

-- -------------------------------------------------------------------------------------------------------
-- 
-- @section 2. Eliminaci�n de base de datos
-- 
-- -------------------------------------------------------------------------------------------------------

-- Eliminaci�n de bases de datos
--DROP DATABASE IF EXISTS ${hiveconf:ENV}_FUNCTIONAL CASCADE;

-- -------------------------------------------------------------------------------------------------------
-- 
-- @section 3. Creaci�n de base de datos
-- 
-- -------------------------------------------------------------------------------------------------------

-- Creaci�n de base de datos
CREATE DATABASE IF NOT EXISTS ${hiveconf:ENV}_FUNCTIONAL LOCATION '/user/${hiveconf:ENV}/datalake/${hiveconf:ENV}_FUNCTIONAL';


-- -------------------------------------------------------------------------------------------------------

-- COMANDO DE EJECUCION
-- beeline -u jdbc:hive2:// -f /home/userbda000/Laboratorio_022_proceso_tunning_codigo_y_tuning_recursos.sql --hiveconf "ENV=userbda000"

-- -------------------------------------------------------------------------------------------------------
-- 
-- @section 1. Definici�n de par�metros
-- 
-- -------------------------------------------------------------------------------------------------------

-- [HIVE] Creamos una variable en HIVE
-- SET ENV=userbda000;

-- -------------------------------------------------------------------------------------------------------
-- 
-- @section 2. Tunning
-- 
-- -------------------------------------------------------------------------------------------------------

-- Compresi�n
SET hive.exec.compress.output=true;
SET parquet.compression=SNAPPY;

-- Particionamiendo din�mico
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=9999;
SET hive.exec.max.dynamic.partitions.pernode=9999;

-- Selecci�n del motor de ejecuci�n
SET hive.execution.engine=mr;
-- SET hive.execution.engine=spark;
-- SET hive.execution.engine=tez;

-- Tunning de recursos computacionales [mr]
SET mapreduce.job.maps=8;
SET mapreduce.map.cpu.vcores=2;
SET mapreduce.map.memory.mb=1024;
SET mapreduce.job.reduces=8;
SET mapreduce.reduce.cpu.vcores=2;
SET mapreduce.reduce.memory.mb=1024;
SET mapreduce.input.fileinputformat.split.maxsize = 1024000000;
SET mapreduce.input.fileinputformat.split.minsize = 1024000000;

-- Tunning de recursos computacionales [spark]
-- SET spark.driver.memory=1g;
-- SET spark.dynamicAllocation.maxExecutors=8;
-- SET spark.executor.cores=2;
-- SET spark.executor.memory=1g;
--SET spark.executor.memoryOverhead=100m;

-- Tunning de recursos computacionales [tez]
--set mapred.reduce.tasks = -1;

-- -------------------------------------------------------------------------------------------------------
-- 
-- @section 3. Deploy de tablas temporales
-- 
-- -------------------------------------------------------------------------------------------------------
-- Creaci�n de tabla
CREATE TABLE ${hiveconf:ENV}_FUNCTIONAL.TRANSACCION_ENRIQUECIDA(
	ID_PERSONA INT,
	NOMBRE_PERSONA STRING,
	EDAD_PERSONA INT,
	SALARIO_PERSONA DOUBLE,
	TRABAJO_PERSONA STRING,
	MONTO_TRANSACCION DOUBLE,
	EMPRESA_TRANSACCION STRING
)
PARTITIONED BY (FECHA_TRANSACCION STRING)
STORED AS PARQUET
LOCATION '/user/${hiveconf:PARAM_USERNAME}/datalake/${hiveconf:ENV}_FUNCTIONAL/TRANSACCION_ENRIQUECIDA'
TBLPROPERTIES(
    'store.charset'='ISO-8859-1', 
    'retrieve.charset'='ISO-8859-1',
    'parquet.compression'='SNAPPY'
);


-- Eliminamos la tabla temporal
DROP TABLE IF EXISTS ${hiveconf:ENV}_FUNCTIONAL.TMP_TRANSACCION_ENRIQUECIDA_1;

-- Creamos la tabla temporal
CREATE TEMPORARY TABLE ${hiveconf:ENV}_FUNCTIONAL.TMP_TRANSACCION_ENRIQUECIDA_1(
    ID_PERSONA STRING,
    NOMBRE_PERSONA STRING,
    EDAD_PERSONA INT,
    SALARIO_PERSONA DOUBLE,
    ID_EMPRESA_PERSONA STRING,
    MONTO_TRANSACCION DOUBLE,
    FECHA_TRANSACCION STRING,
    ID_EMPRESA_TRANSACCION STRING
)
STORED AS PARQUET;

-- Eliminamos la tabla temporal
DROP TABLE IF EXISTS ${hiveconf:ENV}_FUNCTIONAL.TMP_TRANSACCION_ENRIQUECIDA_2;

-- Creamos la tabla temporal
CREATE TEMPORARY TABLE ${hiveconf:ENV}_FUNCTIONAL.TMP_TRANSACCION_ENRIQUECIDA_2(
    ID_PERSONA STRING,
    NOMBRE_PERSONA STRING,
    EDAD_PERSONA INT,
    SALARIO_PERSONA DOUBLE,
    TRABAJO_PERSONA STRING,
    MONTO_TRANSACCION DOUBLE,
    FECHA_TRANSACCION STRING,
    ID_EMPRESA_TRANSACCION STRING
)
STORED AS PARQUET;

-- Eliminamos la tabla temporal
DROP TABLE IF EXISTS ${hiveconf:ENV}_FUNCTIONAL.TMP_TRANSACCION_ENRIQUECIDA_3;

-- Creamos la tabla temporal
CREATE TEMPORARY TABLE ${hiveconf:ENV}_FUNCTIONAL.TMP_TRANSACCION_ENRIQUECIDA_3(
    ID_PERSONA STRING,
    NOMBRE_PERSONA STRING,
    EDAD_PERSONA INT,
    SALARIO_PERSONA DOUBLE,
    TRABAJO_PERSONA STRING,
    MONTO_TRANSACCION DOUBLE,
    FECHA_TRANSACCION STRING,
    EMPRESA_TRANSACCION STRING
)
STORED AS PARQUET;

-- -------------------------------------------------------------------------------------------------------
-- 
-- @section 4. Proceso
-- 
-- -------------------------------------------------------------------------------------------------------

-- Truncamos la tabla
TRUNCATE TABLE ${hiveconf:ENV}_FUNCTIONAL.TRANSACCION_ENRIQUECIDA;

-- PASO 1: OBTENER LOS DATOS DE LA PERSONA QUE REALIZ� LA TRANSACCI�N
INSERT INTO ${hiveconf:ENV}_FUNCTIONAL.TMP_TRANSACCION_ENRIQUECIDA_1
    SELECT
        T.ID_PERSONA,
        P.NOMBRE,
        P.EDAD,
        P.SALARIO,
        P.ID_EMPRESA,
        T.MONTO,
        T.FECHA,
        T.ID_EMPRESA
    FROM
        ${hiveconf:ENV}_curated.TRANSACCION T
            JOIN ${hiveconf:ENV}_curated.PERSONA P
            ON T.ID_PERSONA = P.ID;

-- Verificamos
SELECT * FROM ${hiveconf:ENV}_FUNCTIONAL.TMP_TRANSACCION_ENRIQUECIDA_1 LIMIT 10;

-- PASO 2: OBTENER EL NOMBRE DE LA EMPRESA EN DONDE TRABAJA LA PERSONA
INSERT INTO ${hiveconf:ENV}_FUNCTIONAL.TMP_TRANSACCION_ENRIQUECIDA_2
    SELECT
        T.ID_PERSONA,
        T.NOMBRE_PERSONA,
        T.EDAD_PERSONA,
        T.SALARIO_PERSONA,
        E.NOMBRE,
        T.MONTO_TRANSACCION,
        T.FECHA_TRANSACCION,
        T.ID_EMPRESA_TRANSACCION
    FROM
        ${hiveconf:ENV}_FUNCTIONAL.TMP_TRANSACCION_ENRIQUECIDA_1 T
            JOIN ${hiveconf:ENV}_curated.EMPRESA E
            ON T.ID_EMPRESA_PERSONA = E.ID;

-- Verificamos
SELECT * FROM ${hiveconf:ENV}_FUNCTIONAL.TMP_TRANSACCION_ENRIQUECIDA_2 LIMIT 10;

-- PASO 3: OBTENER EL NOMBRE DE LA EMPRESA EN DONDE SE REALIZ� LA TRANSACCI�N
INSERT INTO ${hiveconf:ENV}_FUNCTIONAL.TMP_TRANSACCION_ENRIQUECIDA_3
    SELECT
        T.ID_PERSONA,
        T.NOMBRE_PERSONA,
        T.EDAD_PERSONA,
        T.SALARIO_PERSONA,
        T.TRABAJO_PERSONA,
        T.MONTO_TRANSACCION,
        T.FECHA_TRANSACCION,
        E.NOMBRE
    FROM
        ${hiveconf:ENV}_FUNCTIONAL.TMP_TRANSACCION_ENRIQUECIDA_2 T
            JOIN ${hiveconf:ENV}_curated.EMPRESA E
            ON T.ID_EMPRESA_TRANSACCION = E.ID;

-- Verificamos
SELECT * FROM ${hiveconf:ENV}_FUNCTIONAL.TMP_TRANSACCION_ENRIQUECIDA_3 LIMIT 10;

-- PASO 4: INSERTAR EN LA TABLA RESULTANTE FINAL
INSERT OVERWRITE TABLE ${hiveconf:ENV}_FUNCTIONAL.TRANSACCION_ENRIQUECIDA
PARTITION (FECHA_TRANSACCION)
    SELECT
        T.ID_PERSONA,
        T.NOMBRE_PERSONA,
        T.EDAD_PERSONA,
        T.SALARIO_PERSONA,
        T.TRABAJO_PERSONA,
        T.MONTO_TRANSACCION,
        T.EMPRESA_TRANSACCION,
        T.FECHA_TRANSACCION
    FROM
        ${hiveconf:ENV}_FUNCTIONAL.TMP_TRANSACCION_ENRIQUECIDA_3 T;

-- Verificamos
SELECT * FROM ${hiveconf:ENV}_FUNCTIONAL.TRANSACCION_ENRIQUECIDA LIMIT 10;

-- -------------------------------------------------------------------------------------------------------
-- 
-- @section 4. Eliminaci�n de tablas temporales
-- 
-- -------------------------------------------------------------------------------------------------------

-- DROP TABLE IF EXISTS ${hiveconf:ENV}_FUNCTIONAL.TMP_TRANSACCION_ENRIQUECIDA_1;
-- DROP TABLE IF EXISTS ${hiveconf:ENV}_FUNCTIONAL.TMP_TRANSACCION_ENRIQUECIDA_2;
-- DROP TABLE IF EXISTS ${hiveconf:ENV}_FUNCTIONAL.TMP_TRANSACCION_ENRIQUECIDA_3;