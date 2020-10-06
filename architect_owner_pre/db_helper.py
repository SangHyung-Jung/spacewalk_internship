import psycopg2 as pg
import psycopg2.extras
import pandas.io.sql as psql
import os
import pandas as pd
from sqlalchemy import create_engine

class DB_HELPER():
    def __init__(self):
        self.pg_local = {
            'host' : os.environ['AWS_HOST'],
            'user' : os.environ['AWS_USER'],
            'password' : os.environ['AWS_PW']
        }

    # from db read tables to pandas
    def read_tables(self, dbname, table_name):

        connect_string = "host={host} user={user} password = {password}"\
        .format(**self.pg_local) + " dbname = {}".format(dbname)

        connection = pg.connect(connect_string)
        df = pd.read_sql_query('select * from {}'.format(table_name), con=connection)
        connection.close()

        return df

    # DataFrame to db ( new table )
    def save_tables(self, df, dbname, table_name):

        engine_string = 'postgresql+psycopg2://{user}:{password}@{host}:5432/'.format(**self.pg_local) + '{}'.format(dbname)
        engine = create_engine(engine_string)

        df.to_sql('{}'.format(table_name), con = engine, if_exists = 'fail', index = False)

    # DataFrame alter ( delete the old table and write new table )
    def alter_tables(self, df, dbname, table_name):

        engine_string = 'postgresql+psycopg2://{user}:{password}@{host}:5432/'.format(**self.pg_local) + '{}'.format(dbname)
        engine = create_engine(engine_string)

        df.to_sql('{}'.format(table_name), con = engine, if_exists = 'replace', index = False)

    # Data append
    def append_tables(self, df, dbname, table_name):

        engine_string = 'postgresql+psycopg2://{user}:{password}@{host}:5432/'.format(**self.pg_local) + '{}'.format(dbname)
        engine = create_engine(engine_string)

        df.to_sql('{}'.format(table_name), con = engine, if_exists = 'append', index = False)
