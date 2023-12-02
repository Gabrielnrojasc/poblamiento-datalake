-- -------------------------------------------------------------------------------------------------------
-- COMANDO DE EJECUCION
-- beeline -u jdbc:hive2:// -f /home/gabrielrc/procesos/Poblando_Capa_Workload.sql --hiveconf "PARAM_USERNAME=gabrielrc" --hiveconf "ENV=DEV" 

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
DROP DATABASE IF EXISTS ${hiveconf:ENV}_workload CASCADE;


-- -------------------------------------------------------------------------------------------------------
-- 
-- @section 3. Creaci�n de base de datos
-- 
-- -------------------------------------------------------------------------------------------------------

-- Creaci�n de base de datos
CREATE DATABASE IF NOT EXISTS ${hiveconf:ENV}_workload LOCATION '/user/gabrielrc/datalake/${hiveconf:ENV}_workload/';

-- -------------------------------------------------------------------------------------------------------
-- 
-- @section 4. Despliegue de tabla PERSONA
-- 
-- -------------------------------------------------------------------------------------------------------

-- Creaci�n de tabla
-- Creaci�n de tabla PERSONA

CREATE TABLE ${hiveconf:ENV}_workload.PERSONA(
	ID STRING,
	NOMBRE STRING,
	TELEFONO STRING,
	CORREO STRING,
	FECHA_INGRESO STRING,
	EDAD STRING,
	SALARIO STRING,
	ID_EMPRESA STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/${hiveconf:PARAM_USERNAME}/datalake/${hiveconf:ENV}_workload/persona'
TBLPROPERTIES(
    'skip.header.line.count'='1', 
    'store.charset'='ISO-8859-1', 
    'retrieve.charset'='ISO-8859-1'
);


-- Subida de datos
LOAD DATA LOCAL INPATH '/home/${hiveconf:PARAM_USERNAME}/dataset/persona.data'
INTO TABLE ${hiveconf:ENV}_workload.PERSONA;

-- Impresi�n de datos
SELECT * FROM ${hiveconf:ENV}_workload.PERSONA LIMIT 10;


-- -------------------------------------------------------------------------------------------------------
-- 
-- @section 5. Despliegue de tabla EMPRESA
-- 
-- -------------------------------------------------------------------------------------------------------

-- Creaci�n de tabla EMPRESA

CREATE TABLE ${hiveconf:ENV}_workload.EMPRESA(
	ID STRING,
	NOMBRE STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/${hiveconf:PARAM_USERNAME}/datalake/${hiveconf:ENV}_workload/empresa'
TBLPROPERTIES(
    'skip.header.line.count'='1',
    'store.charset'='ISO-8859-1', 
    'retrieve.charset'='ISO-8859-1'
);

-- Subida de datos
LOAD DATA LOCAL INPATH '/home/${hiveconf:PARAM_USERNAME}/dataset/empresa.data'
INTO TABLE ${hiveconf:ENV}_workload.EMPRESA;

-- Impresi�n de datos
SELECT * FROM ${hiveconf:ENV}_workload.EMPRESA LIMIT 10;

-- -------------------------------------------------------------------------------------------------------
-- 
-- @section 6. Despliegue de tabla TRANSACCION
-- 
-- -------------------------------------------------------------------------------------------------------

-- Creaci�n de tabla Trasacciones

CREATE TABLE ${hiveconf:ENV}_workload.TRANSACCION(
	ID_PERSONA STRING,
	ID_EMPRESA STRING,
	MONTO STRING,
	FECHA STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/${hiveconf:PARAM_USERNAME}/datalake/${hiveconf:ENV}_workload/transaccion'
TBLPROPERTIES(
    'skip.header.line.count'='1',
    'store.charset'='ISO-8859-1', 
    'retrieve.charset'='ISO-8859-1'
);

-- Subida de datos
LOAD DATA LOCAL INPATH '/home/${hiveconf:PARAM_USERNAME}/dataset/transacciones.data'
INTO TABLE ${hiveconf:ENV}_workload.TRANSACCION;

-- Impresi�n de datos
SELECT * FROM ${hiveconf:ENV}_workload.TRANSACCION LIMIT 10;
