"""
create_at: 2024-12-11 13:04:00
edited_at: 2024-01-20 07:53:00

author: ronald.barberi
read me: Para la ejecución de determinadas funciones administrativas, debe estar disponible la clase '_cls_sqlalchemy.py'.
    Ya está integrado para la ejecución múltiples acciones, optimizando y mejorando el rendimiento.
"""

#%% Imported libraries

import os
import pandas as pd
from datetime import datetime
from _cls_sqlalchemy import SqlAchemy as sa

#%% Create class

class PySparkKretoN:

    @staticmethod
    def funConfigEnviron():
        """
        Esta función configura todo el ambiente de PySprak para poderlo utilizar en el proyecto.

        Parámetros:
        No recibe ningúno sin embargo es fundamental el validar y configurar las rutas dentro de
        esta propia función y debe de inicializarse siempre que se utiliza PySpark.
        
        Retorna:
        No retnorna ningún atributo, más si deja el ambiente totalmente configurado para su uso.
        """
        varDicCurrent = os.path.abspath(os.path.dirname(__file__))

        os.environ['JAVA_HOME'] = r'C:\tools\jdk'
        os.environ['PATH'] += os.pathsep + os.path.join(os.environ['JAVA_HOME'], 'bin')

        os.environ['HADOOP_HOME'] = r'C:\tools\hadoop'
        os.environ['hadoop.home.dir'] = r'C:\tools\hadoop'
        os.environ['PATH'] += os.pathsep + os.path.join(os.environ['HADOOP_HOME'], 'bin')

        os.environ['PYSPARK_PYTHON'] = os.path.join(varDicCurrent, '..', 'venv', 'Scripts', 'python.exe')
        os.environ['PYSPARK_DRIVER_PYTHON'] = os.path.join(varDicCurrent, '..', 'venv', 'Scripts', 'python.exe')
        
        os.environ['JAR_SQLSERVER'] = r'C:\tools\jdbc\sqlserver_jar\enu\jars\mssql-jdbc-12.8.1.jre11.jar'
        os.environ['PATH'] += os.pathsep + r'C:\tools\jdbc\sqlserver_jar\enu\auth\x64'
        os.environ['JAR_ORACLE'] = r'C:\tools\jdbc\oracle\ojdbc8.jar'        
        os.environ['JAR_POSTGRESQL'] = r'C:\tools\jdbc\postgresql\postgresql-42.7.5.jar'
        os.environ['JAR_MYSQL'] = r'C:\tools\jdbc\mysql\mysql.jar'
        os.environ['JAR_EXCEL'] = r'C:\tools\jdbc\excel\spark-excel_2.12-3.5.1_0.20.4.jar'

        if 'JAVA_HOME' in os.environ and 'HADOOP_HOME' in os.environ:
            print('Config environ succes.')
        else:
            print('Config environ failed.')
    

    
    def funCreateRDDToSQL(sparkSession: str, url: str, properties: dict, pathQuery: str):
        """
        Esta función genera un DataFrame en PySprak en base a una consulta SQL.

        Parámetros:
        sparkSession (str): La sesión de PySpark que se está utilizando para el proceso.
        url (str): La url de conexión a la base de datos.
        properties (dict): El diccionario de properties al generar la conexión con la base de datos.
        pathQuery (str): La ruta en donde se encuentra la consulta (file.sql).
        
        Retorna:
        rdd: El DataFrame generado en base a la consulta.
        """
        with open(pathQuery, 'r', encoding='Latin1') as file:
            sql_query = file.read()

        rdd = sparkSession.read \
            .format('jdbc') \
            .option('url', url) \
            .option('dbtable', f'({sql_query})') \
            .option('user', properties['user']) \
            .option('password', properties['password']) \
            .option('driver', properties['driver']) \
            .option('charset', 'Latin1') \
            .option('encoding', 'Latin1') \
            .load()
        
        rdd.printSchema()
        return rdd



    def funCreateRDDToXslx(sparkSession, pathFile: str, dtypesDf=None, dtypesSpark=None):
        """
         Esta función genera un DataFrame en PySprak en base a una archivo Excel, usando Pandas en ves del JAR.

        Parámetros:
        sparkSession (str): La sesión de PySpark que se está utilizando para el proceso.
        pathQuery (str): La ruta en donde se encuentra el archivo excel (file.xlsx).
        dtypesDf=None: Diccionario de para ingresar el tipo de dato correspondiente en Pandas al DataFrame.
        dtypesSpark=None: Diccionario de para ingresar el tipo de dato correspondiente en PySpark al DataFrame.
        
        Retorna:
        rdd: El DataFrame generado en base al archivo Excel.
        """
        if dtypesDf:
            df = pd.read_excel(pathFile, dtype=dtypesDf)
        else:
            df = pd.read_excel(pathFile)
        
        if dtypesSpark:
            rdd = sparkSession.createDataFrame(df, schema=dtypesSpark)
        else:
            rdd = sparkSession.createDataFrame(df)
        rdd.printSchema()

        return rdd



    def funPrintData(varDf):
        varDf.printSchema()
        varDf.show(10)



    def funExportDataToXlsx(df, pathExport: str, dicDtypes=None):
        """
         Esta función exporta un DataFrame creado en PySpark en un archivo Excel, usando Pandas en ves del JAR.

        Parámetros:
        df: El DataFrame generado que se requiere exportar en xslx.
        pathExport (str): La ruta en donde se exportará el archivo excel (file.xlsx).
        dicDtypes=None: Diccionario de para ingresar el tipo de dato correspondiente en Pandas al DataFrame.
        """
        pandas_df = df.toPandas()
        if dicDtypes:
            for column, dtype in dicDtypes.items():
                if column in pandas_df.columns:
                    pandas_df[column] = pandas_df[column].astype(dtype)

        pandas_df.to_excel(pathExport, index=False)



    def funConectSQLSpark(motorDataBase: str,
                          host: str,
                          port: int,
                          dataBase: str,
                          user: str,
                          password: str
    ):
        # Insert the commando in you Script: .config("spark.jars", os.environ['SPARK_JARS']) \
        """
        Esta función establece una conexión con una base de datos específica utilizando Spark y JDBC.

        Parámetros:
        motorDataBase (str): El tipo de motor de base de datos (puede ser 'sqlserver', 'oracle', 'postgresql', 'mysql').
        host (str): La dirección del servidor de la base de datos.
        port (int): El puerto del servidor de la base de datos.
        dataBase (str): El nombre de la base de datos a la que se conectará.
        user (str): El nombre de usuario para autenticarse en la base de datos.
        password (str): La contraseña para el usuario proporcionado.

        Retorna:
        tuple: Una tupla que contiene:
            - url (str): La URL de conexión JDBC para el motor de base de datos específico.
            - properties (dict): Un diccionario con las propiedades de la conexión (host, puerto, base de datos, usuario, contraseña, driver).
            - engine (object): El motor de conexión SQLAlchemy para la base de datos proporcionada.
        """
        if motorDataBase == 'sqlserver':    
            url = (
                f'jdbc:sqlserver://{host}:{port};databaseName={dataBase};user={user};password={password};'
                'encrypt=true;'
                'trustServerCertificate=true;'
                'charset=utf8'
            )
            driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
            
        elif motorDataBase == 'oracle':
            url = f'jdbc:oracle:thin:@{host}:{port}/{dataBase}'
            driver = 'oracle.jdbc.OracleDriver'
        
        elif motorDataBase == 'postgresql':
            url = f'jdbc:postgresql://{host}:{port}/{dataBase}'
            driver = 'org.postgresql.Driver'
            

        elif motorDataBase == 'mysql':
            url = f'jdbc:mysql://{host}:{port}/{dataBase}?useSSL=false&serverTimezone=UTC'
            driver = 'com.mysql.cj.jdbc.Driver'
        
        else:
            raise ValueError(f'Invalid option: {motorDataBase}')
            
        properties = {
            'host': host,
            'port': port,
            'dataBase': dataBase,
            'user': user,
            'password': password,
            'driver': driver
        }
        
        engine = sa.funConectServer(motorDataBase, host, port, dataBase, user, password)

        return url, properties, engine



    def funCreateTableToRDD(motorDataBase: str,
                            df,
                            table: str,
                            url: str,
                            properties: str,
                            schema=None
        ):
        """
        Esta función crea una tabla en la base de datos de manera rapida creandola desde cero (borrará la tabla si ya existe).

        Parámetros:
        df: El DataFrame con el cual se estre trabajando en el momento.
        schema: str: La dirección del servidor de la base de datos.
        port (int): El puerto del servidor de la base de datos.
        dataBase (str): El nombre de la base de datos a la que se conectará.
        user (str): El nombre de usuario para autenticarse en la base de datos.
        password (str): La contraseña para el usuario proporcionado.

        Retorna:
        tuple: Una tupla que contiene:
            - url (str): La URL de conexión JDBC para el motor de base de datos específico.
            - properties (dict): Un diccionario con las propiedades de la conexión (host, puerto, base de datos, usuario, contraseña, driver).
            - engine (object): El motor de conexión SQLAlchemy para la base de datos proporcionada.
        """
        valid_motors = {'sqlserver', 'oracle', 'postgresql', 'mysql'}
        if motorDataBase not in valid_motors:
            raise ValueError(f'Motor de base de datos inválido. Debe ser uno de: {valid_motors}')
        
        table_formats = {
            'sqlserver': f"[{properties['dataBase']}].[{schema}].[{table}]",
            'oracle': f"{properties['dataBase']}.{schema}.{table}",
            'postgresql': f"\"{properties['dataBase']}\".\"{schema}\".\"{table}\"",
            'mysql': f"`{properties['dataBase']}`.`{table}`"
        }
        fullTableName = table_formats[motorDataBase] if schema or motorDataBase == 'mysql' else table

        try:
            df.write.jdbc(url=url,
                          table=fullTableName,
                          mode='overwrite',
                          properties=properties
            )
        except Exception as e:
            print('Error creating table', e)



    def funInsertOnDuplicateKeyUpdate(motorDataBase: str,
                                      sparkSession: str,
                                      url: str,
                                      properties: dict,
                                      engine: str,
                                      df,
                                      table: str,
                                      schema=None,
                                      chunk: int = 10_000
        ):
        schema_condition = f"AND tc.TABLE_SCHEMA = '{schema}'" if schema else ""
        query_to_pks = f"""
            SELECT 
                c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE c 
                ON tc.CONSTRAINT_NAME = c.CONSTRAINT_NAME
            WHERE tc.TABLE_NAME = '{table}'
                {schema_condition}
                AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        """

        rdd_pks = sparkSession.read \
            .format('jdbc') \
            .option('url', url) \
            .option('query', query_to_pks) \
            .options(**properties) \
            .load()
        column_name = rdd_pks.columns[0]
        columns_pk = [getattr(fila, column_name) for fila in rdd_pks.collect()]

        columns_into_motordb = {
            'sqlserver': ', '.join([col for col in df.columns]),
            'oracle': ', '.join([f'"{col}"' for col in df.columns]),
            'postgresql': ', '.join([f'"{col}"' for col in df.columns]),
            'mysql': ', '.join([f'`{col}`' for col in df.columns]),
        }
        cols_into = columns_into_motordb.get(motorDataBase, ', '.join([col for col in df.columns]))

        columns_oncondition_motordb = {
            'sqlserver': ' AND '.join([f'I.[{col}] = II.[{col}]' for col in columns_pk]),
            'oracle': ' AND '.join([f'I.[{col}] = II.[{col}]' for col in columns_pk]),
            'postgresql': ', '.join([f'"{col}"' for col in columns_pk]),
            'mysql': ' AND '.join([f'I.[{col}] = II.[{col}]' for col in columns_pk]),
        }
        cols_on_condition = columns_oncondition_motordb.get(motorDataBase, ', '.join([col for col in df.columns]))

        columns_updateset_motordb = {
            'sqlserver': ', '.join([f'I.[{col}] = II.[{col}]' for col in df.columns]),
            'oracle': ', '.join([f'I.[{col}] = II.[{col}]' for col in df.columns]),
            'postgresql': ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in df.columns]),
            'mysql': ', '.join([f'I.[{col}] = II.[{col}]' for col in df.columns]),
        }
        cols_update_set = columns_updateset_motordb.get(motorDataBase, ', '.join([col for col in df.columns]))


        records = df.rdd.map(tuple).collect()
        for i in range(0, len(records), chunk):
            batch = records[i:i + chunk]
            values = ','.join([
                '(' + ','.join(
                    f"'{v}'" if isinstance(v, (str, datetime)) else ('NULL' if v is None else str(v))
                    for v in record
                ) + ')'
                for record in batch
            ])

            if motorDataBase == 'sqlserver' or motorDataBase == 'oracle':
                query_to_funtion = f"""
                MERGE INTO [{properties['dataBase']}].[{schema}].[{table}] AS I
                USING (VALUES
                    {values}
                ) AS II ({cols_into})
                    ON {cols_on_condition}
                WHEN MATCHED THEN
                    UPDATE SET
                        {cols_update_set}
                WHEN NOT MATCHED THEN
                    INSERT ({cols_into})
                    VALUES (SELECT * FROM II);
                """

            elif motorDataBase == 'oracle':
                query_to_funtion = f"""
                MERGE INTO \"{properties['dataBase']}\".\"{schema}\".\"{table}\" I
                USING (SELECT * FROM (VALUES {values})) II ({cols_into})
                ON ({cols_on_condition})
                WHEN MATCHED THEN
                    UPDATE SET {cols_update_set}
                WHEN NOT MATCHED THEN
                    INSERT ({cols_into})
                    VALUES (SELECT * FROM II)
                """

            elif motorDataBase == 'postgresql':
                query_to_funtion = f"""
                INSERT INTO \"{properties['dataBase']}\"."{schema}"."{table}" (
                    {cols_into}
                )
                VALUES {values}
                ON CONFLICT ({cols_on_condition})
                DO UPDATE SET
                    {cols_update_set};
                """
                
            elif motorDataBase == 'mysql':
                query_to_funtion = f"""
                REPLACE INTO `{properties['dataBase']}`.`{table}` AS I ({cols_into})
                VALUES({values})

                """

            else:
                raise ValueError(f'The database engine is not within the options: {motorDataBase}')

            sa.funExecuteQuery(engine, query_to_funtion)
