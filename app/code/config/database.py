import sys
import os
from sqlalchemy import create_engine,text

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'..','python')))

from settings import get_settings



settings = get_settings()


def connect_db(is_mysql):
        """
        Create a connection with database

        Parameters:
        - is_mysql (bool): specifiy the database type \n
            True => mysql \n
            False => postgres
        Returns:
        - engine object
        """
        ## is_mysql = True => connect to ((MYSQL DB))
        ## is_mysql = False => connect to ((POSTGRES DB))
        
        ## Connection Configuration
        host = settings['mysql_host'] if is_mysql else settings['postgres_host']
        user = settings['mysql_user'] if is_mysql else settings['postgres_user']
        password = settings['mysql_password'] if is_mysql else settings['postgres_password']
        database = settings['mysql_db'] if is_mysql else settings['postgres_db']
        port = settings['mysql_port'] if is_mysql else settings['postgres_port']
        db_engine = 'mysql+pymysql' if is_mysql else 'postgresql+psycopg2'

        connect_status = True
        try:
            engine = create_engine(f"{db_engine}://{user}:{password}@{host}:{port}/{database}", isolation_level="AUTOCOMMIT")
            print('Connected successfully')
        except Exception as e:
            print('Error while tying to connect to Database \n', e)
            connect_status = False

        return engine,connect_status


print('test')
print(settings)