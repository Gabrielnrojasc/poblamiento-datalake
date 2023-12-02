-- -------------------------------------------------------------------------------------------------------

-- COMANDO DE EJECUCION
-- beeline -u jdbc:hive2:// -f /home/gabrielrc/procesos/Poblando_Capa_Landing.sql --hiveconf "PARAM_USERNAME=gabrielrc" --hiveconf "ENV=DEV" 

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
DROP DATABASE IF EXISTS ${hiveconf:ENV}_landing CASCADE;

-- -------------------------------------------------------------------------------------------------------
-- 
-- @section 3. Creaci�n de base de datos
-- 
-- -------------------------------------------------------------------------------------------------------

-- Creaci�n de base de datos
CREATE DATABASE IF NOT EXISTS ${hiveconf:ENV}_LANDING LOCATION '/user/${hiveconf:PARAM_USERNAME}/datalake/${hiveconf:ENV}_LANDING';


-- -------------------------------------------------------------------------------------------------------
-- 
-- @section 4. Tunning
-- 
-- -------------------------------------------------------------------------------------------------------

-- Compresi�n
SET hive.exec.compress.output=true;
SET avro.output.codec=snappy;

-- Particionamiendo din�mico
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- -------------------------------------------------------------------------------------------------------
-- 
-- @section 5. Despliegue de tabla PERSONA
-- 
-- -------------------------------------------------------------------------------------------------------

-- Creaci�n de tabla
CREATE TABLE ${hiveconf:ENV}_LANDING.PERSONA
STORED AS AVRO
LOCATION '/user/${hiveconf:PARAM_USERNAME}/datalake/${hiveconf:ENV}_LANDING/persona'
TBLPROPERTIES(
    'store.charset'='ISO-8859-1', 
    'retrieve.charset'='ISO-8859-1',
    'avro.schema.url'='hdfs:///user/${hiveconf:PARAM_USERNAME}/datalake/schema/${hiveconf:ENV}_LANDING/persona.avsc',
	'avro.output.codec'='snappy'
);

-- Inserci�n de datos
INSERT INTO TABLE ${hiveconf:ENV}_LANDING.PERSONA
SELECT * FROM  ${hiveconf:ENV}_workload.PERSONA;

-- Impresi�n de datos
SELECT * FROM ${hiveconf:ENV}_LANDING.PERSONA LIMIT 10;

-- -------------------------------------------------------------------------------------------------------
-- 
-- @section 6. Despliegue de tabla EMPRESA
-- 
-- -------------------------------------------------------------------------------------------------------

-- Creaci�n de tabla
CREATE TABLE ${hiveconf:ENV}_LANDING.EMPRESA
STORED AS AVRO
LOCATION '/user/${hiveconf:PARAM_USERNAME}/datalake/${hiveconf:ENV}_LANDING/empresa'
TBLPROPERTIES(
    'store.charset'='ISO-8859-1', 
    'retrieve.charset'='ISO-8859-1',
    'avro.schema.url'='hdfs:///user/${hiveconf:PARAM_USERNAME}/datalake/schema/${hiveconf:ENV}_LANDING/empresa.avsc',
	'avro.output.codec'='snappy'
);

-- Inserci�n de datos
INSERT INTO TABLE ${hiveconf:ENV}_LANDING.EMPRESA
SELECT * FROM  ${hiveconf:ENV}_workload.EMPRESA;  

-- Impresi�n de datos
SELECT * FROM ${hiveconf:ENV}_LANDING.EMPRESA LIMIT 10;

-- -------------------------------------------------------------------------------------------------------
-- 
-- @section 7. Despliegue de tabla TRANSACCION
-- 
-- -------------------------------------------------------------------------------------------------------

-- Creaci�n de tabla
-- Creaci�n de tabla TRANSACCION
CREATE TABLE ${hiveconf:ENV}_LANDING.TRANSACCION
PARTITIONED BY (FECHA STRING)
STORED AS AVRO
LOCATION '/user/${hiveconf:PARAM_USERNAME}/datalake/${hiveconf:ENV}_LANDING/transaccion'
TBLPROPERTIES(
    'store.charset'='ISO-8859-1', 
    'retrieve.charset'='ISO-8859-1',
    'avro.schema.url'='hdfs:///user/${hiveconf:PARAM_USERNAME}/datalake/schema/${hiveconf:ENV}_LANDING/transaccion.avsc',
	'avro.output.codec'='snappy'
);

-- Inserci�n de datos por particionamiento din�mico
INSERT INTO TABLE ${hiveconf:ENV}_LANDING.TRANSACCION
PARTITION(FECHA)
SELECT * FROM  ${hiveconf:ENV}_workload.TRANSACCION;

-- Impresi�n de datos
SELECT * FROM ${hiveconf:ENV}_LANDING.TRANSACCION LIMIT 10;

-- Verificamos las particiones
SHOW PARTITIONS ${hiveconf:ENV}_LANDING.TRANSACCION;