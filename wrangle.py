import pandas as pd
import numpy as np
import math

import acquire

def get_lower_and_upper_bounds(col, multiplier):
    
    '''
    Function for the continuous probabilistic methods to identify outliers (using multiplier of 1.5)
    '''
    multiplier = 1.5
    
    q1 = col.quantile(0.25)
    q3 = col.quantile(0.75)

    iqr = q3 - q1
    
    lower_bound = q1 - multiplier * iqr
    upper_bound = q3 + multiplier * iqr
    
    return lower_bound, upper_bound


def parse_api_logs_data(entry):
    '''
    Function for Discrete Anomally detection
    '''

#     df = acquire.get_logs_data()

    sections = entry.split()

    output = {'ip': sections[0],
#               'timestamp': [i.replace(':', ' ', 1) for i in sections[3][1:]],
              'timestamp': sections[3][1:].replace(':', ' ', 1),
              'request_method': sections[5][1:],
              'request_path': sections[6],
              'http_version': sections[7][:-1],
              'status_code': sections[8],
              'size': int(sections[9]),
              'user_agent': ''.join(sections[11:]).replace('"', '')
             }

    return pd.Series(output)


def value_count_and_proba(s: pd.Series, dropna = True) -> pd.DataFrame:
    '''
    Function that creates value counts and frequencies
    '''
    return pd.merge(
        s.value_counts(dropna=False).rename('count'),
        s.value_counts(dropna=False, normalize=True).rename('proba'),
        left_index=True,
        right_index=True,
    )
    

def parse_grocery_data():
    '''
    Function that parse grocery data for clustering anomally exercises
    '''
    
    df = acquire.get_grocery_data()
    # index customer id column
    df = df.set_index('customer_id').sort_index()
    # Make all names lower
    df.columns = [x.lower() for x in df.columns]
    
    return df


def parse_curriculum_access_data():
    '''
    Function for Detecting anomally through clustering
    '''
    df = acquire.get_curriculum_logs()
    
    # Fill nulls with 0
    df = df.fillna(0)
       
    # Convert timestamp to datetime format
    df.date = pd.to_datetime(df.date)
    
    # index customer id column
    df = df.set_index('date').sort_index()
    
    # Initial rename col name 
    df = df.rename(columns = {0:'curriculum'})
    
    return df




