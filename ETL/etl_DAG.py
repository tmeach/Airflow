import pandas as pd 
import requests 
from datetime import datetime, timedelta
from io import StringIO
import pandahouse as ph

from airflow.decorators import dag, task 
from airflow.operators.python import get_current_context


# функция для подключения к CH
def ch_get_df(query='Select 1', host='https://clickhouse.lab.karpov.courses', user='student', password='dpo_python_2020'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result

# подключение к базе test в CH
connection_test = {'host':'https://clickhouse.lab.karpov.courses', 
              'user':'student-rw', 
              'password':'656e2b0c9c',
              'database': 'test'}

# Дефолтные параметры
default_args = {
    'owner': 't-pitsuev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 14),
}

# Интервал запуска
schedule_interval = '0 07 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_tpitsuev():
    @task()
    def extract_feed():
        query = """SELECT 
                        user_id,
                        toDate(time) as event_date,
                        gender,
                        os,
                        age,
                        countIf(action = 'like') as likes,
                        countIf(action = 'view') as views
                    FROM simulator_20221020.feed_actions
                    GROUP BY user_id, event_date, gender, os, age
                    format TSVWithNames"""
            
        df_feed = ch_get_df (query = query)
        return df_feed
    @task()
    def extract_msg():
        query = """SELECT
                        user_id,
                        date as event_date,
                        gender,
                        os,
                        age,
                        messages_sent,
                        users_sent,
                        messages_received,
                        users_received
                    FROM
                    (SELECT 
                        user_id,
                        toDate(time) as date,
                        gender,
                        os,
                        age,
                        COUNT() as messages_sent,
                        uniqExact(reciever_id) as users_sent
                    FROM simulator_20221020.message_actions
                    GROUP BY user_id, date, gender, os, age
                    HAVING toDate(time) = today() - 1) t1

                    FULL OUTER JOIN

                    (SELECT 
                        reciever_id as user_id,
                        toDate(time) as date,
                        gender,
                        os,
                        age,
                        COUNT(user_id) as messages_received,
                        uniqExact(user_id) as users_received
                     FROM simulator_20221020.message_actions
                     GROUP BY user_id, date, gender, os, age
                     HAVING toDate(time) = today() - 1) t2
                     USING user_id
                     format TSVWithNames"""
        
        df_msg = ch_get_df (query = query)
        return df_msg
    
    @task()
    def union_df(df_feed, df_msg):
        df = df_feed.merge(df_msg, how = 'outer', on = ['user_id','event_date','gender','age','os']).dropna()
        return df
    
    @task()
    def transform_gender(df):
        df_gender = df[['event_date','gender','likes', 'views', 'messages_sent', 'messages_received', 'users_received', 'users_sent']]\
        .groupby(['gender','event_date'])\
        .sum()\
        .reset_index()
        df_gender['dimension'] = 'gender'
        df_gender.rename(columns = {'gender':'dimension_value'}, inplace = True)
        return df_gender
                        
    @task()
    def transform_age(df):
        df_age = df[['event_date','age','likes', 'views', 'messages_sent', 'messages_received', 'users_received', 'users_sent']]\
        .groupby(['age','event_date'])\
        .sum()\
        .reset_index()
        df_age['dimension'] = 'age'
        df_age.rename(columns = {'age':'dimension_value'}, inplace = True)
        return df_age

    @task()
    def transform_os(df):
        df_os = df[['event_date','os','likes', 'views', 'messages_sent', 'messages_received', 'users_received', 'users_sent']]\
        .groupby(['os','event_date'])\
        .sum()\
        .reset_index()
        df_os['dimension'] = 'os'
        df_os.rename(columns = {'os':'dimension_value'}, inplace = True)
        return df_os

    @task
    def union_all(df_gender, df_age, df_os):
        concat_df = pd.concat([df_gender, df_age, df_os]).reset_index(drop=True)
        concat_df = concat_df.astype({'event_date' : 'datetime64',
                                        'dimension' : 'str',
                                        'dimension_value' : 'str',
                                        'views' : 'int64',
                                        'likes' : 'int64',
                                        'messages_sent' : 'int64',
                                        'users_received' : 'int64',
                                        'messages_received' : 'int64',
                                        'users_sent' : 'int64'})
        
        return concat_df
    
    @task
    def load (concat_df):
        q = '''
        CREATE TABLE IF NOT EXISTS test.tpitsuev_etl_lesson
        (event_date Date,
        dimension String,
        dimension_value String,
        views Int64,
        likes Int64,
        messages_sent Int64,
        users_received Int64,
        messages_received Int64,
        users_sent Int64
        ) 
        ENGINE = MergeTree
        order by event_date
        partition by toYYYYMMDD(event_date)
        '''
        ph.execute(query=q, connection=connection_test)
        ph.to_clickhouse(df=concat_df, table = 'tpitsuev_etl_lesson', connection = connection_test, index = False)
    
    df_feed = extract_feed()
    df_msg = extract_msg()
    df = union_df(df_feed, df_msg)
    
    df_gender = transform_gender(df)
    df_age = transform_age(df)
    df_os = transform_os(df)
    
    concat_df = union_all(df_gender, df_age, df_os)
    load(concat_df)

dag_tpitsuev = dag_tpitsuev()
