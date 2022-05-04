import env
import pandas as pd
import os

def db_conn(db):

#     db = 'logs'
    url = f'mysql+pymysql://{env.user}:{env.password}@{env.host}/{db}'
    return url

def get_lemonade_data():
    
    url = 'https://gist.githubusercontent.com/ryanorsinger/19bc7eccd6279661bd13307026628ace/raw/e4b5d6787015a4782f96cad6d1d62a8bdbac54c7/lemonade.csv'
    
    df = pd.read_csv(url)
    
    df.columns = df.columns.str.lower()
    
    # Convert to datetime and set index on date
    df = df.set_index(pd.to_datetime(df.date))
    
    df['month'] = df.index.month_name()
    
    return df


def get_api_logs_data(use_cache = True):
    '''
    Function to acquire data for discrete anomally detection
    '''
    db = 'logs'

    file_name = '.ipynb_checkpoints/logs.csv'

    if os.path.exists(file_name) and use_cache:

        print('Getting data from local machine..')

        return pd.read_csv(file_name)

    qry = 'SELECT * FROM api_access;'

    print('Getting data from SQL..')

    df = pd.read_sql(qry, db_conn(db))

    print('Saving local csv file locally..')

    df.to_csv(file_name, index= False)

    return df


def get_curriculum_logs():
    '''
    Function to acquire curriculum.txt data for clustering anomally detection
    '''
#     url = 'https://classroom.google.com/c/NDQ2NzY2NDY1ODM0/p/NTEwNzA4MTAwNzEw/details'

    df = pd.read_table('curriculum-access.txt', header=None, sep = '\s', 
                       names = ['date', 'time', 'page', 'id', 'cohort', 'ip'])

    return df


def get_grocery_data(use_cache = True):
    '''
    Function to acquire data for detecting anomally using Density Based Clustering Exercises
    '''
    db = 'grocery_db'

    file_name = 'grocery.csv'

    if os.path.exists(file_name) and use_cache:

        print('Getting grocery data from local machine..')

        return pd.read_csv(file_name)

    qry = 'SELECT * FROM grocery_customers;'

    print('Getting grocery data from SQL..')

    df = pd.read_sql(qry, db_conn(db))

    print('Saving local csv file locally..')

    df.to_csv(file_name, index= False)

    return df





