-- -------------------------------------------------------------------------------------------------------

-- COMANDO DE EJECUCION
-- beeline -u jdbc:hive2:// -f /home/gabrielrc/procesos/Poblando_Capa_Curated.sql --hiveconf "PARAM_USERNAME=gabrielrc" --hiveconf "ENV=DEV" 

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
--DROP DATABASE IF EXISTS ${hiveconf:ENV}_curated CASCADE;

-- -------------------------------------------------------------------------------------------------------
-- 
-- @section 3. Creaci�n de base de datos
-- 
-- -------------------------------------------------------------------------------------------------------

-- Creaci�n de base de datos
CREATE DATABASE IF NOT EXISTS ${hiveconf:ENV}_curated LOCATION '/user/${hiveconf:PARAM_USERNAME}/datalake/${hiveconf:ENV}_curated';

-- -------------------------------------------------------------------------------------------------------
-- 
-- @section 4. Tunning
-- 
-- -------------------------------------------------------------------------------------------------------

-- Compresi�n
SET hive.exec.compress.output=true;
SET parquet.compression=SNAPPY;

-- Particionamiendo din�mico
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- -------------------------------------------------------------------------------------------------------
-- 
-- @section 5. Despliegue de tabla PERSONA
-- 
-- -------------------------------------------------------------------------------------------------------

-- Creaci�n de tabla PERSONA
CREATE TABLE ${hiveconf:ENV}_curated.PERSONA(
    ID STRING,
    NOMBRE STRING,
    TELEFONO STRING,
    CORREO STRING,
    FECHA_INGRESO STRING,
    EDAD INT,
    SALARIO DOUBLE,
    ID_EMPRESA STRING
)
STORED AS PARQUET
LOCATION '/user/${hiveconf:PARAM_USERNAME}/datalake/${hiveconf:ENV}_curated/PERSONA'
TBLPROPERTIES(
    'store.charset'='ISO-8859-1', 
    'retrieve.charset'='ISO-8859-1',
    'parquet.compression'='SNAPPY'
);

-- Inserci�n, casteo de datos y aplicacion de reglas de limpieza
INSERT INTO TABLE ${hiveconf:ENV}_curated.PERSONA
    SELECT
        CAST(T.ID AS STRING),
        CAST(T.NOMBRE AS STRING),
        CAST(T.TELEFONO AS STRING),
        CAST(T.CORREO AS STRING),
        CAST(T.FECHA_INGRESO AS STRING),
        CAST(T.EDAD AS INT),
        CAST(T.SALARIO AS DOUBLE),
        CAST(T.ID_EMPRESA AS STRING)
    FROM 
        ${hiveconf:ENV}_LANDING.PERSONA T
    WHERE 
        T.ID IS NOT NULL AND
        T.ID_EMPRESA IS NOT NULL AND
        CAST(T.EDAD AS INT) > 0 AND
        CAST(T.EDAD AS INT) < 100 AND
        CAST(T.SALARIO AS DOUBLE) > 0 AND
        CAST(T.SALARIO AS DOUBLE) < 10000000;

-- Impresi�n de datos
SELECT * FROM ${hiveconf:ENV}_curated.PERSONA LIMIT 10;

-- -------------------------------------------------------------------------------------------------------
-- 
-- @section 6. Despliegue de tabla EMPRESA
-- 
-- -------------------------------------------------------------------------------------------------------

-- Creaci�n de tabla
CREATE TABLE ${hiveconf:ENV}_curated.EMPRESA(
    ID STRING,
    NOMBRE STRING
)
STORED AS PARQUET
LOCATION '/user/${hiveconf:PARAM_USERNAME}/datalake/${hiveconf:ENV}_curated/EMPRESA'
TBLPROPERTIES(
    'store.charset'='ISO-8859-1', 
    'retrieve.charset'='ISO-8859-1',
    'parquet.compression'='SNAPPY'
);

-- Inserci�n, casteo de datos y aplicacion de reglas de limpieza
INSERT INTO TABLE ${hiveconf:ENV}_curated.EMPRESA
    SELECT
        CAST(T.ID AS STRING),
        CAST(T.NOMBRE AS STRING)
    FROM 
        ${hiveconf:ENV}_LANDING.EMPRESA T
    WHERE 
        T.ID IS NOT NULL; 

-- Impresi�n de datos
SELECT * FROM ${hiveconf:ENV}_curated.EMPRESA LIMIT 10;

-- -------------------------------------------------------------------------------------------------------
-- 
-- @section 7. Despliegue de tabla TRANSACCION
-- 
-- -------------------------------------------------------------------------------------------------------

-- Creaci�n de tabla
CREATE TABLE ${hiveconf:ENV}_curated.TRANSACCION(
    ID_PERSONA STRING,
    ID_EMPRESA STRING,
    MONTO DOUBLE
)
PARTITIONED BY (FECHA STRING)
STORED AS PARQUET
LOCATION '/user/${hiveconf:PARAM_USERNAME}/datalake/${hiveconf:ENV}_CURATED/TRANSACCION'
TBLPROPERTIES(
    'store.charset'='ISO-8859-1', 
    'retrieve.charset'='ISO-8859-1',
    'parquet.compression'='SNAPPY'
);

-- Inserci�n por particionamiento din�mico, casteo de datos y aplicacion de reglas de limpieza
INSERT INTO TABLE ${hiveconf:ENV}_curated.TRANSACCION
PARTITION(FECHA)
    SELECT
        CAST(T.ID_PERSONA AS STRING),
        CAST(T.ID_EMPRESA AS STRING),
        CAST(T.MONTO AS DOUBLE),
        CAST(T.FECHA AS STRING)
    FROM 
        ${hiveconf:ENV}_LANDING.TRANSACCION T
    WHERE 
        T.ID_PERSONA IS NOT NULL AND
        T.ID_EMPRESA IS NOT NULL AND
        CAST(T.MONTO AS DOUBLE) >= 0;

-- Impresi�n de datos
SELECT * FROM ${hiveconf:ENV}_curated.TRANSACCION LIMIT 10;

-- Verificamos las particiones
SHOW PARTITIONS ${hiveconf:ENV}_curated.TRANSACCION;