import pandas as pd
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


# Настройки для Telegram бота
my_token = '***' 
bot = telegram.Bot(token=my_token)
chat_id = ***

# Функция подключения к ClickHouse
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


# Параметры для DAG
default_args = {
    'owner': 't-pitsuev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 15),
}

# Расписание DAGa
schedule_interval = '0 11 * * *'

# DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def feed_report():
    @task
    def extract_data():
        """
        Извлечение данных из ClickHouse.

        Returns:
            pandas.DataFrame: DataFrame с данными.
        """
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
        try:
            df = ch_get_df(query)
            return df
        except RequestException as e:
            error_msg = f"An error occurred while extracting data from ClickHouse: {str(e)}"
            send_telegram_message(error_msg)
            return None
    
    @task
    def create_text_report(df):
        """
        Создание текстового отчета и отправка в Telegram.

        Args:
            df (pandas.DataFrame): DataFrame с данными.

        Returns:
            None
        """
        DAU = df['DAU'].iloc[0]
        views = df['views'].iloc[0]
        likes = df['likes'].iloc[0]
        CTR = round(df['CTR'].iloc[0], 2)
        LPU = round(df['LPU'].iloc[0], 2)
        report_date = datetime.now().date() - timedelta(days=1)

        # Текстовый отчет
        msg = f'💼 Лента новостей. Отчет за {report_date}:\n \n 🚶 DAU: {DAU}\n 👀 Просмотры: {views}\n 💔 Лайки: {likes}\n 🎯 CTR: {CTR}\n 🥰 LPU: {LPU}'
        bot.sendMessage(chat_id=chat_id, text=msg)
        
        return
    
    @task
    def create_visual_report(df):
        """
        Создание визуального отчета и отправка в Telegram.

        Args:
            df (pandas.DataFrame): DataFrame с данными.

        Returns:
            None
        """
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
    text_report(df)
    visual_report(df)
    
feed_report = feed_report()
