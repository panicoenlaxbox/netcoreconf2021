# netcoreconf

El índice de este documento es parejo a las secciones en la presentación.

## Instalar Spark en Windows
https://www.panicoenlaxbox.com/post/install-spark-on-windows/

## Python

Para los 2 primeros ejemplos en Python es necesario crear un entorno virtual.

```
cd PythonApp1
python -m venv env
env\Scripts\activate
pip install pyspark
env\Scripts\deactivate
```

A continuación, con `code .` puedes abrir VSCode y ejecutar cada uno de los ficheros, o bien hacerlo desde la línea de comandos con `python main.py` y `python main2.py`

## Databricks

Tendrás que crear lo siguiente:

- Un clúster de Databricks con la versión *9.0 (includes Apache Spark 3.1.2, Scala 2.12)*
- Una cuenta de almacenamiento de tipo ADLS Gen2. 
- Un Service Principal con permisos de *Storage blob data contributor* sobre la cuenta anterior.

## .NET for Apache Spark

Requiere .NET Core 3.1

## Databricks Connect

La versión del clúster de Databricks tiene que ser *8.1 (includes Apache Spark 3.1.1, Scala 2.12)*

Tanto  la ejecución de Python como la de .NET for Apache Spark, hay que hacerlo desde la línea de comandos con el entorno virtual activado y habiendo eliminado la variable de entorno `SPARK_HOME` y establecido la variabld de entorno `PYSPARK_PYTHON`.