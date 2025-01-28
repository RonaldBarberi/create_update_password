"""
create_at: 2025-01-20 21:04:00
edited_at: 2025-01-27 16:50:00

author: ronald.barberi
"""

#%% Imported libraries

import os
import shutil
import random
from datetime import datetime
from dotenv import load_dotenv
import win32com.client as win32
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from _cls_sqlalchemy import SqlAchemy as sa
from _cls_pyspark_own import PySparkKretoN as psk
from pyspark.sql.functions import current_timestamp

#%% Create class

class CreateUpdatePass:
    def __init__(self, dicArgs):
        psk.funConfigEnviron()
        self.dicArgs = dicArgs
        self.spark = SparkSession.builder \
                .appName('CreateUpdatePass_pyspark') \
                .config('spark.jars', os.environ['JAR_POSTGRESQL']) \
                .enableHiveSupport() \
                .getOrCreate()


    def create_save_new_password(self):
        url, properties, engine = psk.funConectSQLSpark('postgresql',
                                            self.dicArgs['host'],
                                            self.dicArgs['port'],
                                            self.dicArgs['dataBase'],
                                            self.dicArgs['user'],
                                            self.dicArgs['password']
        )
        sa.funExecuteQuery(engine, self.dicArgs['spExecute'])
        
        rdd = psk.funCreateRDDToSQL(self.spark,
                                    url,
                                    properties,
                                    self.dicArgs['filePathQuery'],
        )

        def generate_password(id_origin):
            if id_origin in (4, 16):
                return ''.join(random.choice('1234567890') for _ in range(4))
            else:
                return ''.join(random.choice('abcdefghijkmnpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789@#$%*') for _ in range(16))
        
        password_udf = udf(generate_password, StringType())
        rdd = rdd.withColumn('new_password', password_udf(rdd['id_origin']))
        rdd = rdd.withColumn('update_at', current_timestamp())
        rdd = rdd.cache()
        rdd.count()

        query = """
            user,
            new_password AS password,
            id_origin,
            update_at
        """
        expressions = [expr.strip() for expr in query.strip().split(',')]
        rdd = rdd.selectExpr(*expressions)
        psk.funPrintData(rdd)


        rdd.select('id_origin', 'password').show(20)

        psk.funInsertOnDuplicateKeyUpdate('postgresql',
                                          self.spark,
                                          url,
                                          properties,
                                          engine,
                                          rdd,
                                          self.dicArgs['table'],
                                          self.dicArgs['schema']
        )

        if not os.path.exists(self.dicArgs['pathExportTxt']):
            os.makedirs(self.dicArgs['pathExportTxt'])

        rdd.write \
            .format('csv') \
            .option('header', 'true') \
            .option('delimiter', '|') \
            .option('timestampFormat', 'yyyy-MM-dd HH:mm:ss') \
            .mode('overwrite') \
            .save(self.dicArgs['pathExportTxt'])

        for file in os.listdir(self.dicArgs['pathExportTxt']):
            if file.endswith('.csv'):
                shutil.move(os.path.join(self.dicArgs['pathExportTxt'], file), os.path.join(self.dicArgs['pathExportTxt'], 'passwords.txt'))


    def send_new_password(self):
        outlook_app = win32.Dispatch('Outlook.Application')
        outlook_app.GetNamespace('MAPI')
        message = outlook_app.CreateItem(0)
        message.To = self.dicArgs['Destinatario']
        message.CC = self.dicArgs['Copia']
        message.BCC = self.dicArgs['CopiaOculta']
        message.Subject = self.dicArgs['Asunto']
        message.HTMLBody = f"""
        <html>
            <body>
                <p>Buen día,</p>
                <p>{self.dicArgs['Mensaje']}</p>
                <p>Greetings!,</p>
                <p>Ronald Barberi,</p>
                <p>Ing de Datos</p>
            </body>
        </html>
        """
        attachment_path = os.path.join(self.dicArgs['pathExportTxt'], 'passwords.txt')
        message.Attachments.Add(attachment_path)
        try:
            message.Send()
            print('Mensaje enviado exitosamente.')
            shutil.rmtree(self.dicArgs['pathExportTxt'])
        except Exception as e:
            print('Error al enviar el correo, por favor validar:', e)

    def main(self):
        self.create_save_new_password()
        self.send_new_password()

#%% Use class

if __name__ == "__main__":
    load_dotenv()
    periodo_now = datetime.now().strftime('%Y%m')
    varDicCurrent = os.path.abspath(os.path.dirname(__file__))
    dicArgs = {
        'filePathQuery': os.path.join(varDicCurrent, '..', 'sql', '_sql_extract_credentials.sql'),
        'host': os.getenv('HOST_POSTGRESQL'),
        'port': os.getenv('PORT_POSTGRESQL'),
        'dataBase': 'bbdd_kreton_learning',
        'schema': 'sch_itl',
        'table': 'tb_credentials',
        'user': os.getenv('USER_POSTGRESQL'),
        'password': os.getenv('PASSWORD_POSTGRESQL'),
        'spExecute': 'SELECT sp_credentials_backup();',
        'pathExportTxt': os.path.join(varDicCurrent, '..', 'data', 'tmp_credentials'),
        'Destinatario': os.getenv('EMAIL'),
        'Copia': 'crreo123@gmail.com;',
        'CopiaOculta': '',
        'Asunto': f'Actualización de credenciales periodo {periodo_now}.',
        'Mensaje': f'Se comparte credenciales actualizadas para el periodo {periodo_now}. <br>',
    }
    bot_pass = CreateUpdatePass(dicArgs)
    bot_pass.main()
