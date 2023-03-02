import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import requests
import pandahouse

from datetime import date, timedelta, datetime
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

sns.set()

default_args = {
    'owner': 'a.burlakov-9',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 16)
    }

schedule_interval = '*/15 * * * *'

def check_anomaly(df, metric, a=4, n=6):
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['up'] = df['q75'] + a*df['iqr']
    df['low'] = df['q25'] - a*df['iqr']
    
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
     
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    
    return is_alert, df

def run_alerts_feed(chat=None):
    chat_id = chat or -5203XXXXXXXXX
    bot = telegram.Bot(token='6023168328:AAE1WuD5RUDLNDyOgGdJkPBmKsXXXXXXXXXXXXXX')
    
    connection = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'password': 'XXXXXXXXXXXXX',
        'user': 'student',
        'database': 'simulator_20230120'
    }

    query = ''' SELECT
                    toStartOfFifteenMinutes(time) as ts,
                    toDate(ts) as date,
                    formatDateTime(ts, '%R') as hm,
                    uniqExact(user_id) as users_lenta,
                    countIf(user_id,action='view') as views,
                    countIf(user_id, action='like') as likes,
                    round(likes/views,3) as CTR
                FROM simulator_20230120.feed_actions
                WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
                GROUP BY ts, date, hm
                ORDER BY ts
            '''

    data = pandahouse.read_clickhouse(query, connection=connection) 
    
    metrics_list = ['users_lenta','views','likes','CTR']
    for metric in metrics_list:
        #print(metric)
        df = data[['ts', 'date', 'hm', metric]].copy()
        is_alert, df = check_anomaly(df, metric)
        
        if is_alert == 1:
            msg = '''Метрика {metric}:\n текущее значение = {current_val:.2f}\n отклонение от вчера {last_val_diff:.2%}\n https://superset.lab.karpov.courses/superset/dashboard/2842/'''.format(metric=metric, current_val=df[metric].iloc[-1], last_val_diff=abs(1 - (df[metric].iloc[-1]/df[metric].iloc[-2])))
            
            sns.set(rc={'figure.figsize': (16, 10)})  
            plt.tight_layout()
        
            ax = sns.lineplot(x=df['ts'], y=df[metric], label='metric')
            ax = sns.lineplot(x=df['ts'], y=df['up'], label='up')
            ax = sns.lineplot(x=df['ts'], y=df['low'], label='low')
        
            for ind, label in enumerate(ax.get_xticklabels()): 
                if ind % 15 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)    
                
            ax.set(xlabel='time') 
            ax.set(ylabel=metric) 
        
            ax.set_title('{}'.format(metric)) 
            ax.set(ylim=(0, None)) 

            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()
        
            bot.sendMessage(chat_id=chat_id, text=msg)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)

def run_alerts_message(chat=None):
    chat_id = chat or -5203XXXXXXXXXX
    bot = telegram.Bot(token='6023168328:AAE1WuD5RUDLNDyOgGdJkPBmKXXXXXXXXXXXXXXXXXX')
   
    connection = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'password': 'XXXXXXXXXXXXX',
        'user': 'student',
        'database': 'simulator_20230120'
    }

    query = ''' SELECT
                    toStartOfFifteenMinutes(time) as ts,
                    formatDateTime(ts, '%F') as date,
                    formatDateTime(ts, '%R') as hm,
                    uniqExact(user_id) as users_messages,
                    count(user_id) as sent_messages
                FROM simulator_20230120.message_actions
                WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
                GROUP BY ts, date, hm
                ORDER BY ts
            '''

    data = pandahouse.read_clickhouse(query, connection=connection) 
    
    metrics_list = ['users_messages','sent_messages']
    for metric in metrics_list:
        #print(metric)
        df = data[['ts', 'date', 'hm', metric]].copy()
        is_alert, df = check_anomaly(df, metric)
        
        if is_alert == 1:
            msg = '''Метрика {metric}:\n текущее значение = {current_val:.2f}\n отклонение от вчера {last_val_diff:.2%}\n https://superset.lab.karpov.courses/superset/dashboard/2842/'''.format(metric=metric, current_val=df[metric].iloc[-1], last_val_diff=abs(1 - (df[metric].iloc[-1]/df[metric].iloc[-2])))
            
   

            sns.set(rc={'figure.figsize': (16, 10)})  
            plt.tight_layout()
        
            ax = sns.lineplot(x=df['ts'], y=df[metric], label='metric')
            ax = sns.lineplot(x=df['ts'], y=df['up'], label='up')
            ax = sns.lineplot(x=df['ts'], y=df['low'], label='low')
        
            for ind, label in enumerate(ax.get_xticklabels()): 
                if ind % 15 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)    
                
            ax.set(xlabel='time') 
            ax.set(ylabel=metric) 
        
            ax.set_title('{}'.format(metric)) 
            ax.set(ylim=(0, None)) 

            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()
        
            bot.sendMessage(chat_id=chat_id, text=msg)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)
       
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def burlakov_dag_alert():

    @task
    def check():
        try:
            run_alerts_feed()
        except Exception as e:
            print(e)
    
        try:
            run_alerts_message()
        except Exception as e:
            print(e)
       
    check()
    
burlakov_dag_alert = burlakov_dag_alert()
            
