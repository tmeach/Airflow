import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import pandahouse
import telegram
from datetime import date
import io
from read_db.CH import Getch 
import sys 
import os


# Дефолтные параметры DAG
default_args = {
    'owner': 't-pitsuev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 21),
}

# Интервал запуска
schedule_interval = '0 11 * * *'

# Параметры Бота
my_token = '5642886625:AAHn-dqXSiuBZHvlQbRvXBbAITmQRJVqfgY'
bot = telegram.Bot(token=my_token)


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def alert_system():
    @task()
    def check_anomaly_feed(df, metric, a=4, n=5):
        # функция предлагает алгоритм поиска аномалий в данных (межквартильный размах)
        df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
        df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
        df['iqr'] = df['q75']-df['q25']
        df['up'] = df['q75']+ a*df['iqr'] 
        df['low'] = df['q25'] - a*df['iqr']

        df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
        df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()

    
        if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
            is_alert = 1
        else:
            is_alert = 0

        return is_alert, df

    @task
    def run_alerts_feed(chat=None):
        # непосредственно сама система алертов
        chat_id = chat or -715927362
        bot = telegram.Bot(token = '5642886625:AAHn-dqXSiuBZHvlQbRvXBbAITmQRJVqfgY')

        data = Getch('''SELECT
                            toStartOfFifteenMinutes(time) as ts,
                            toDate(time) as date,
                            formatDateTime(ts, '%R') as hm, 
                            uniqExact(user_id) as users_feed, 
                            COUNTIf(user_id, action = 'view') as views,
                            COUNTIf(user_id, action = 'like') as likes,
                            COUNTIf(action = 'like')/countIf(action = 'view') as CTR
                        FROM simulator_20221020.feed_actions
                        WHERE time >= today() - 1 and time < toStartOfFifteenMinutes(now())
                        GROUP BY ts, date, hm
                        ORDER BY ts''').df
        print(data)

        metrics_list = ['users_feed', 'views', 'likes', 'CTR']
        for metric in metrics_list:
            print(metric)
            df = data[['ts','date', 'hm', metric]].copy()
            is_alert, df = check_anomaly_feed(df, metric)

            if is_alert == 1 or True:
                msg = '''📌Метрика {metric}:\n 👀Текущее значение: {current_val:.2f}\n 📉Отклонение от предыдущего значения: {last_val_diff:.2%}\n https://superset.lab.karpov.courses/superset/dashboard/2212/'''.format(metric=metric, current_val=df[metric].iloc[-1], last_val_diff=abs(1 - (df[metric].iloc[-1]/df[metric].iloc[-2]))) 
                sns.set(rc={'figure.figsize':(16, 10)})
                plt. tight_layout()
                ax = sns.lineplot(x=df['ts'], y = df[metric], label = metric)
                ax = sns.lineplot(x=df['ts'], y = df['up'], label = 'up')
                ax = sns.lineplot(x=df['ts'], y = df['low'], label = 'low')

                for ind, label in enumerate(ax.get_xticklabels()): 
                    if ind%2 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)

                ax.set(xlabel = 'time')
                ax.set(ylabel = metric)

                ax.set_title(metric)
                ax.set(ylim=(0, None))


                plot_object = io.BytesIO() 
                plt.savefig(plot_object) 
                plot_object.seek(0)
                plot_object.name = 'test_plot.jpg'
                plt.close()


                bot.sendMessage(chat_id=chat_id, text=msg)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)

        return

    @task
    def check_anomaly_msg(dff, metric, a=4, n=5):
        # функция предлагает алгоритм поиска аномалий в данных (межквартильный размах)
        dff['q25'] = dff[metric].shift(1).rolling(n).quantile(0.25)
        dff['q75'] = dff[metric].shift(1).rolling(n).quantile(0.75)
        dff['iqr'] = dff['q75']-dff['q25']
        dff['up'] = dff['q75']+ a*dff['iqr'] 
        dff['low'] = dff['q25'] - a*dff['iqr']

        dff['up'] = dff['up'].rolling(n, center=True, min_periods=1).mean()
        dff['low'] = dff['low'].rolling(n, center=True, min_periods=1).mean()


        if dff[metric].iloc[-1] < dff['low'].iloc[-1] or dff[metric].iloc[-1] > dff['up'].iloc[-1]:
            is_alert = 1
        else:
            is_alert = 0

        return is_alert, dff
    
    @task
    def run_alerts_msg(chat=None):
        chat_id = chat or -715927362
        bot = telegram.Bot(token = '5642886625:AAHn-dqXSiuBZHvlQbRvXBbAITmQRJVqfgY')

        data_msg = Getch('''SELECT
                            toStartOfFifteenMinutes(time) as ts,
                            toDate(time) as date,
                            formatDateTime(ts, '%R') as hm, 
                            uniqExact(user_id) as users_message, 
                            count(reciever_id) AS "Messages"
                        FROM simulator_20221020.message_actions
                        WHERE time >= today() - 1 and time < toStartOfFifteenMinutes(now())
                        GROUP BY ts, date, hm
                        ORDER BY ts''').df
        print(data_msg)

        metrics_list = ['users_message', 'Messages']
        for metric in metrics_list:
            print(metric)
            dff = data_msg[['ts','date', 'hm', metric]].copy()
            is_alert, dff = check_anomaly_msg(dff, metric)

            if is_alert == 1 or True:
                msg = '''📌Метрика {metric}:\n 👀Текущее значение: {current_val:.2f}\n 📉Отклонение от предыдущего значения: {last_val_diff:.2%}\n https://superset.lab.karpov.courses/superset/dashboard/2212/'''.format(metric=metric, current_val=dff[metric].iloc[-1], last_val_diff=abs(1 - (dff[metric].iloc[-1]/dff[metric].iloc[-2])))  

                sns.set(rc={'figure.figsize':(16, 10)})
                plt. tight_layout()

                ax = sns.lineplot(x=dff['ts'], y = dff[metric], label = metric)
                ax = sns.lineplot(x=dff['ts'], y = dff['up'], label = 'up')
                ax = sns.lineplot(x=dff['ts'], y = dff['low'], label = 'low')

                for ind, label in enumerate(ax.get_xticklabels()): 
                    if ind%2 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)

                ax.set(xlabel = 'time')
                ax.set(ylabel = metric)

                ax.set_title(metric)
                ax.set(ylim=(0, None))


                plot_object = io.BytesIO() 
                plt.savefig(plot_object) 
                plot_object.seek(0)
                plot_object.name = 'test_plot.jpg'
                plt.close()


                bot.sendMessage(chat_id=chat_id, text=msg)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)

        return
        
    check_anomaly_feed=check_anomaly_feed(df, metric, a=4, n=5)
    run_alerts_feed=run_alerts_feed()
    check_anomaly_msg=check_anomaly_msg(dff, metric, a=4, n=5)
    run_alerts_msg=run_alerts_msg()


    
alert_system = alert_system()
