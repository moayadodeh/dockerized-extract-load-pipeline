import os
import sys
import pandas as pd
from datetime import date,timedelta
import time
from sqlalchemy import text
from io import StringIO
import logging

# Configure logging
logging.basicConfig(
    filename='app.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S' 
)

from settings import get_settings
settings = get_settings()

sys.path.append(os.path.abspath(os.path.join(settings['code_dir'],'config')))
from database import connect_db


class ElClass:

    def __init__(self):
        self.mysql_table = 'consultation'
        self.postgres_table = self.mysql_table + '_pg'
        self.postgres_table_temp = self.postgres_table + '_temp'

        self.last_date_file_path = settings['main_dir']+ '/config/last_date.txt'
        self.data_file_path = settings['data_dir']+'/data_to_load.csv'


    def create_scheme(self,table_name):
        """
        Create a Table with required scheme in the database

        Returns:
        - none
        """
        
        engine,_ = connect_db(False)

        with engine.connect() as conn:

            try:
                path = settings['code_dir']+'/sql/create_table.sql'
                sql = self.read_sql_query(path)

                conn.execute(text(sql.format(table_name=table_name)))
                #conn.commit()
                print("Table created")
            except Exception as e:
                
                print("Table was NOT created",e)


    def read_sql_query(self,file_path):
        print(file_path)
        with open(file_path, 'r') as file:
            sql_query = file.read()
        return sql_query

    
    def extract_load(self,batch_size=500,isIncrement=True):
        """
        Extract data from mysql database 

        Parameters:
        - batch_size (int): the number of rows to deal with in each iteration
        - isIncrement (bool): specify run for the first time or scheduled
        Returns:
        - none
        """

        if(isIncrement==False):
            self.create_scheme(self.postgres_table)
            
            months_ago = 3
            last_date = date.today() - timedelta(months_ago * 30)
            last_date_str = last_date.strftime('%Y-%m-%d')

            start_date = last_date_str

            date_type='date_added'
            try:
                with open(self.last_date_file_path,'w') as last_date:
                    logging.info(start_date)
                    last_date.write('\''+start_date+'\'')
            except Exception as e:
                logging.info("ERROR", e)
        else:
            self.create_scheme(self.postgres_table_temp)
            date_type='updated_date'


        inc = 0

        try:
            with open(self.last_date_file_path,'r') as last_date:
                start_date=last_date.read()
                logging.info('the new date '+start_date)
        except Exception as e:
            logging.info("ERROR: "+e)
        while(True):
            try:
                self.extract_mysql(batch_size,inc,start_date,date_type=date_type)
                self.load_postgres()
                self.clean_up(self.postgres_table_temp)
                logging.info('iter: '+ str(inc)+' done')
                inc+=1
                if(inc >= 50):
                    break
            except Exception as e: 
                logging.info(e)
                break


    def extract_mysql(self,batch_size,iter,start_date,date_type,retries=0,delay_seconds=0):
        """
        Extract data from mysql database 

        Parameters:
        - batch_size (int): the number of rows to deal with in each iteration
        - iter (int): the current iteration
        - start_date (str): the date to start from in the table
        - date_type (str): updated_date => for automated extract 
                           date_added => for manual extract
        - retries (int): Retry counts after failer
        - delay_seconds (int): the delay time between each retryths

        Returns:
        - none
        """

        logging.info('START'+"*"*5 + "EXTRACT MYSQL" + "*"*5)
        logging.info('-'*30)
        

        engine_mysql,connect_status = connect_db(True)
        if connect_status:
            pass
        else:
            for i in range(retries):
                engine_mysql,connect = connect_db(True)
                if connect:
                    break
                else:
                    time.sleep(delay_seconds)
        
                
        def func():
            with engine_mysql.connect() as conn:

                cur = conn.connection.cursor()
                sql = self.read_sql_query(settings['code_dir']+'/sql/fetch_from_mysql.sql').format(
                    table_name=self.mysql_table,
                    date_type=date_type, 
                    startt_date=start_date,
                    batch_size=batch_size,
                    offset=batch_size * iter)
                
                logging.info(batch_size * iter)
                logging.info('start date '+ start_date)

                data = pd.read_sql(sql,conn)
                if(data.shape[0] == 0): 
                    logging.info('size is zero')

                    try:
                        sql = self.read_sql_query(settings['code_dir']+'/sql/get_max_date.sql').format(
                            table_name=self.mysql_table,
                            curr_date = start_date
                        )
                        cur.execute(sql)
                        res = cur.fetchall()
                        with open(self.last_date_file_path,'w') as last_date:
                            logging.info(res[0][0])

                            res_str = res[0][0].strftime('%Y-%m-%d %H:%M:%S')
                            logging.info(res_str)
                            last_date.write('\''+res_str+'\'')
                    except Exception as e:
                        logging.info("ERROR "+ e)

                    logging.info('GET OUT')
                    raise Exception 
                else:
                    logging.info('size is: '+str(data.shape[0]))
                
                data.to_csv(self.data_file_path,index=False)
            return True

        self.retry(func=func(),retries=retries,delay_seconds=delay_seconds)

        logging.info('-'*30)
        logging.info('END'+"*"*5 + "EXTRACT MYSQL" + "*"*5)
        for i in range(5):
            logging.info('-'*30)

    
    #@profile
    def load_postgres(self,retries=0,delay_seconds=0):
        """
        Upload data to postgres database automatically every specific time 

        Parameters:
        - retries (int): Retry counts after failer
        - delay_seconds (int): the delay time between each retry
        
        Returns:
        - none
        """

        def func():
            logging.info('START'+"*"*5 + "LOAD POSTGRES" + "*"*5)
            logging.info('-'*30)

            engine_postgres,connect= connect_db(False)

            with engine_postgres.connect() as conn:
                cur = conn.connection.cursor()
                try:
                    data = pd.read_csv(self.data_file_path)
                except Exception as e:
                    logging.error(e)

                logging.info('data_size: ' + str(data.shape))
                with open(self.last_date_file_path,'r') as last_date:
                    l_date = last_date.read()
                    logging.info(l_date)

                    sio = StringIO()
                    sio1 = StringIO()
                    sio2 = StringIO()


                    #solution = 'A'
                    space = '\t'
                    def solution(sol='C'):
                        if sol == 'B':
                            try:
                                logging.info('-'*15)
                                start = time.time()
                                ## Solution B Delete then Peform full bulk insert

                                ids_list = tuple(data['consultation_id'].astype(int))

                                sql = f""" Select * From {self.postgres_table}
                                        Where consultation_id IN {ids_list}
                                            """
                                intersect_df = pd.read_sql(sql,conn)


                                data_to_delete = data[data['consultation_id'].isin(intersect_df['consultation_id']) == True]
                                print(space,'data_to_delete size:', data_to_delete.shape)
                                if(data_to_delete.shape[0] > 0):
                                    ids_list = tuple(data_to_delete['consultation_id'].astype(int))

                                    sql = text(f""" Delete From {self.postgres_table}
                                            Where consultation_id IN {ids_list} """)

                                    conn.execute(sql)
                                    #deleted_count = conn.rowcount
                                    #conn.commit()
                                    logging.info('Data deleted: '+ str(len(ids_list)))
                                else:
                                    logging.info('Data deleted: '+ '0')

                                print(space,'data to upload size:',data.shape)
                                data.to_csv(sio1,index=None)
                                sio1.seek(0)

                                try:
                                    sql = self.read_sql_query(settings['code_dir']+'/sql/copy_to_table.sql').format(table_name=self.postgres_table)
                                    cur.copy_expert(sql=sql,file=sio1)
                                    #conn.commit()
                                    logging.info('Inserted rows count:'+ str(cur.rowcount))
                                except Exception as e:
                                    print("EERRROR: "+e)
                            except Exception as e:
                                logging.error(e)
                            end = time.time()
                            logging.info('time taken by solution B: '+str(end - start))
                        elif sol == 'C':
                            logging.info('-'*15)

                            start = time.time()

                            try:
                                data.to_csv(sio2,index=None)
                                sio2.seek(0)

                            except Exception as e:
                                logging.ERROR(e)
                            try:
                                sql = self.read_sql_query(settings['code_dir']+'/sql/copy_to_table.sql').format(table_name=self.postgres_table_temp)
#
                                cur.copy_expert(sql=sql,file=sio2)
                                logging.info('rows inserted to ' + self.postgres_table_temp+ " " + str(cur.rowcount))
#
                            except Exception as e:
                                logging.info(e)


                            #try:
                            #    sql = self.read_sql_query(settings['code_dir']+'/sql/test_update.sql')
                            ### update
                            #    logging.info(text(sql.format(table_name=self.postgres_table,table_name_temp=self.postgres_table_temp)))
                            #    cur.execute(sql.format(table_name=self.postgres_table,table_name_temp=self.postgres_table_temp))
                            #except Exception as e:
                            #    logging.info(e)
#
                            #logging.info('TEST7_1')

                            try:
                                sql = self.read_sql_query(settings['code_dir']+'/sql/update_rows.sql')
                            ## update
                                logging.info(text(sql.format(table_name=self.postgres_table,table_name_temp=self.postgres_table_temp)))
                                cur.execute(sql.format(table_name=self.postgres_table,table_name_temp=self.postgres_table_temp))
                            except Exception as e:
                                logging.info(e)

                            #try:
                            #    sql = self.read_sql_query(settings['code_dir']+'/sql/test_insert.sql')
                            #    ## insert
                            #    cur.execute(sql.format(table_name=self.postgres_table,table_name_temp=self.postgres_table_temp))
#
                            #    logging.debug('insert count: ' + str(cur.fetchall()[0]))
                            #except Exception as e:
                            #    logging.info(e)
#
                            #logging.info('TEST8')


                            try:
                                sql = self.read_sql_query(settings['code_dir']+'/sql/insert_to_postgres.sql')
                                ## insert
                                conn.execute(text(sql.format(table_name=self.postgres_table,table_name_temp=self.postgres_table_temp)))
                            except Exception as e:
                                logging.info(e)

                            end = time.time()
                            logging.info('time taken By Solution C: ' + str(end - start))
                    
                    solution('C')

        try:
            func()
        except Exception as e:
            logging.ERROR(e)
            for i in range(int(retries)):
                logging.info(f'Error happend, retry after {delay_seconds} seconds')
                time.sleep(int(delay_seconds))
                func()

    
        logging.info('-'*30)
        logging.info('END'+"*"*5 + "LOAD POSTGRES" + "*"*5)

    def clean_up(self,table_name):
        engine,_ = connect_db(False)
        sql_file_name = 'delete_from_table.sql'

        try:
            os.remove(self.data_file_path)
            sql = self.read_sql_query(settings['code_dir'] + "/sql/" +sql_file_name)
            with engine.connect() as conn:
                conn.execute(text(sql.format(table_name = table_name)))
            logging.info('file removed successfully')
        except:
            logging.info('file does not exists')


    def retry(self,func,retries, delay_seconds):
        try:
            func
        except Exception as e:
            print("ERROR:", e)
            for i in range(int(retries)):
                print(f'Error happend, retry after {delay_seconds} seconds')
                time.sleep(int(delay_seconds))
                if func:
                    break