import pandas as pd 
import requests 
from datetime import datetime, timedelta
from io import StringIO
import pandahouse as ph

from airflow.decorators import dag, task 
from airflow.operators.python import get_current_context

# функция для подключения к CH
def ch_get_df(query='Select 1', host='https://clickhouse.lab.karpov.courses', user='***', password='***'):
    """
    Подключение к ClickHouse и выполнение SQL-запроса.

    Args:
        query (str): SQL-запрос.
        host (str): URL ClickHouse-сервера.
        user (str): Имя пользователя.
        password (str): Пароль.

    Returns:
        pandas.DataFrame: Результат выполнения запроса в виде DataFrame.
    """
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result

# подключение к базе test в CH
connection_test = {'host':'https://clickhouse.lab.karpov.courses', 
              'user':'***', 
              'password':'***',
              'database': '***'}

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
        """
        Извлечение данных из ClickHouse для действий в ленте новостей.

        Returns:
            pandas.DataFrame: DataFrame с данными.
        """
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
        """
        Извлечение данных из ClickHouse для сообщений.

        Returns:
            pandas.DataFrame: DataFrame с данными.
        """
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
        """
        Объединение двух DataFrame.

        Args:
            df_feed (pandas.DataFrame): DataFrame с данными о действиях в ленте новостей.
            df_msg (pandas.DataFrame): DataFrame с данными о сообщениях.

        Returns:
            pandas.DataFrame: Объединенный DataFrame.
        """
        df = df_feed.merge(df_msg, how = 'outer', on = ['user_id','event_date','gender','age','os']).dropna()
        return df
    
    @task()
    def transform_gender(df):
        """
        Преобразование данных по полу пользователей.

        Args:
            df (pandas.DataFrame): Объединенный DataFrame.

        Returns:
            pandas.DataFrame: Преобразованный DataFrame.
        """
        df_gender = df[['event_date','gender','likes', 'views', 'messages_sent', 'messages_received
