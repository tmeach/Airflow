import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import os
import telegram
import requests 
import pandahouse as ph

sns.set()

from datetime import datetime, timedelta
from airflow.decorators import dag, task 
from airflow.operators.python import get_current_context


# функция для подключения к CH

def ch_get_df(query='Select 1', host='https://clickhouse.lab.karpov.courses', user='student', password='dpo_python_2020'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result


# Дефолтные параметры для DAG

default_args = {
    'owner': 't-pitsuev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 15),
}

# Интервал запуска DAG

schedule_interval = '0 11 * * *'

# Задал настройки для бота

my_token = '5642886625:AAHn-dqXSiuBZHvlQbRvXBbAITmQRJVqfgY' # тут нужно заменить на токен вашего бота
bot = telegram.Bot(token=my_token)
chat_id = 68229919

# DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def feed_report():
    @task
    def extract_data():
        query= '''SELECT
                        toFloat64(toDate(time)) as date,
                        COUNT(DISTINCT user_id) as DAU,
                        countIf(action = 'view') as views,
                        countIf(action = 'like') as likes,
                        toInt64(100* countIf(action = 'like') / countIf(action = 'view')) as CTR,
                        toInt64(likes / DAU) as LPU
                    FROM simulator_20221020.feed_actions
                    WHERE toDate(time) BETWEEN today()-8 AND today()-1
                    GROUP BY date
                    LIMIT 10
                    format TSVWithNames'''
        df= ch_get_df (query = query)
        return df
    @task
    def text_report(df):
        DAU = df['DAU'].iloc[0]
        views = df['views'].iloc[0]
        likes = df['likes'].iloc[0]
        CTR = round(df['CTR'].iloc[0],2)
        LPU = round(df['LPU'].iloc[0],2)
        report_date = datetime.now().date() - timedelta(days=1)

        # Текстовый отчет
        msg = f'💼 Лента новостей. Отчет за {report_date}:\n \n 🚶 DAU: {DAU}\n 👀 Просмотры: {views}\n 💔 Лайки: {likes}\n 🎯 CTR: {CTR}\n 🥰 LPU: {LPU}'
        bot.sendMessage(chat_id = chat_id, text = msg)
        
        return
    
    @task
    def visual_report(df):
        fig, axes = plt.subplots(4, 1, figsize=(10, 20))
        fig.suptitle("Значение метрик за предыдущие 7 дней")   

        axes[0].set(title='DAU')
        axes[0].set(xlabel=' ', ylabel=' ')
        sns.lineplot(data=df, ax=axes[0], x='date', y='DAU')

        axes[1].set(title='Likes')
        axes[1].set(xlabel=' ', ylabel=' ')
        sns.lineplot(data=df, ax=axes[1], x="date", y="likes") 

        axes[2].set(title='Views')
        axes[2].set(xlabel=' ', ylabel=' ')
        sns.lineplot(data=df, ax=axes[2], x="date", y="views") 

        axes[3].set(title='CTR')
        axes[3].set(xlabel=' ', ylabel=' ')
        sns.lineplot(data=df, ax=axes[3], x='date', y='CTR')

        plot_object = io.BytesIO() 
        plt.savefig(plot_object) 
        plot_object.seek(0)
        plot_object.name = 'test_plot.jpg'
        plt.close()
        
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        return

    
    
    df = extract_data()
    text_report = text_report(df)
    visual_report = visual_report(df)
    
feed_report = feed_report()  
    
    



