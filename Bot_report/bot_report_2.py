import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import requests 
import pandahouse as ph


from datetime import datetime, timedelta
from airflow.decorators import dag, task 
from airflow.operators.python import get_current_context


# функция для подключения к CH

def ch_get_df(query='Select 1', host='https://clickhouse.lab.karpov.courses', user='***', password='***'):
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

my_token = '***'
bot = telegram.Bot(token=my_token)
chat_id = ***

# DAG

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_feed_msg():
    @task
    def extract():
        query= '''SELECT *
                    FROM(SELECT
                            toDate(time) as date,
                            COUNT(DISTINCT user_id) as DAU_feed,
                            COUNT(DISTINCT post_id) as uniq_post,
                            countIf(action = 'view') as views,
                            countIf(action = 'like') as likes,
                            countIf(action = 'like') / countIf(action = 'view') as CTR
                        FROM simulator_20221020.feed_actions
                        GROUP BY date) t1
                        JOIN
                        (SELECT
                            toDate(time) as date,
                            COUNT(DISTINCT user_id) as DAU_message,
                            COUNT(reciever_id) as messages_amount
                        FROM simulator_20221020.message_actions
                        GROUP BY date) t2
                        USING date
                    WHERE date >= today() - 7
                    AND date < today()
                    ORDER BY date
                    format TSVWithNames'''

        df = ch_get_df (query = query)
        return df
    
    @task
    def send_text_report():
        DAU_feed = df['DAU_feed'].values[0]
        Uniq_post = df['uniq_post'].values[0]
        Views = df['views'].values[0]
        Likes = df['likes'].values[0]
        CTR = round(df['CTR'].values[0],2)
        DAU_message = df['DAU_message'].values[0]
        Messages_amount = df['messages_amount'].values[0]

        report_date = datetime.now().date() - timedelta(days=1)
        

        msg = f'Ключевые метрики за {report_date}:\n \n - Аудитория ленты новостей: {DAU_feed}\n - Уникальные посты: {Uniq_post}\n - Просмотры: {Views}\n - Лайки: {Likes}\n - CTR: {CTR}\n - Аудитория сервиса сообщений: {DAU_message}\n - Количество отправленных сообщений: {Messages_amount}'
        bot.sendMessage(chat_id = chat_id, text = msg) 
        
    @task
    def send_visual_report(): 
        
        # Задаю параметры графиков
        fig, axes = plt.subplots(5, 1, figsize=(10, 20))   
        fig.suptitle("Значение метрик за предыдущие 7 дней")   

        axes[0].set(title='DAU feed')
        axes[0].set(xlabel=' ', ylabel=' ')
        sns.lineplot(data=df, ax=axes[0], x='date', y='DAU_feed')

        axes[1].set(title='DAU message')
        axes[1].set(xlabel=' ', ylabel=' ')
        sns.lineplot(data=df, ax=axes[1], x="date", y="DAU_message") 

        axes[2].set(title='Уникальные посты')
        axes[2].set(xlabel=' ', ylabel=' ')
        sns.lineplot(data=df, ax=axes[2], x="date", y="uniq_post") 

        axes[3].set(title='CTR')
        axes[3].set(xlabel=' ', ylabel=' ')
        sns.lineplot(data=df, ax=axes[3], x='date', y='CTR')


        axes[4].set(title='Количество сообщений')
        axes[4].set(xlabel=' ', ylabel=' ')
        sns.lineplot(data=df, ax=axes[4], x='date', y='messages_amount')

        plot_object = io.BytesIO() # создаем файловый объект
        plt.savefig(plot_object) # сохранили plot_object в файловый объект
        plot_object.seek(0)
        plot_object.name = 'test_plot.jpg'
        plt.close()

        # отправляем фото в бота
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    
    df = extract()


dag_feed_msg = dag_feed_msg()



