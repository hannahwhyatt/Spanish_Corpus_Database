import psycopg2
import configparser
import pandas as pd
from sql_queries import *

# Dataframes
root_path = ''

# main df - texID, seqID, and wID
with open(root_path + 'database/database.txt') as f:
    main_data = [line.strip().split("\t") for line in f.readlines()]
col_names = [name for name in main_data[0] if name]
data = main_data[3:]
main_df = pd.DataFrame(data, columns=col_names)
main_df['ID(seq)'] = main_df['ID(seq)'].astype('Int64')
main_df['wID'] = main_df['wID'].astype('Int64')
main_df['textID'] = main_df['textID'].astype('Int64')

# lexicon df - wID, lemma, pos, and 
with open(root_path+ 'span-samples-lexicon/span-samples-lexicon.txt') as f:
    lexicon_data = [line.strip().split("\t") for line in f.readlines()]
col_names = [name for name in lexicon_data[0] if name]
data = lexicon_data[3:]
lexicon_df = pd.DataFrame(data, columns=col_names)
lexicon_df['wID'] = lexicon_df['wID'].astype('Int64')
lexicon_df['word'] = lexicon_df['word'].astype('str')
lexicon_df['lemma'] = lexicon_df['lemma'].astype('str') 
lexicon_df['PoS'] = lexicon_df['PoS'].astype('str') 

# source df
with open(root_path+'span-samples-sources/span-samples-sources.txt') as f:
    source_data = [line.strip().split("\t") for line in f.readlines()]
col_names = [name for name in source_data[0] if name]
data = source_data[3:]
source_df = pd.DataFrame(data, columns=col_names)
source_df.drop(['website', 'url', 'title'], axis=1, inplace=True)
source_df['textID'] = source_df['textID'].astype('Int64')
source_df['#words'] = pd.to_numeric(source_df['#words'], errors='coerce').astype('Int64')
source_df['genre'] = source_df['genre'].astype('str')
source_df['country'] = source_df['country'].astype('str')
source_df = source_df.fillna(0)


def create_database():
    '''
    create the spanish corpus database 'escorpus'
    return connection and cursor to the database
    '''

    # read config file
    config = configparser.ConfigParser()
    config.read(root_path +'config.cfg')
    DB_NAME_DEFAULT = config.get('SQL', 'DB_NAME_DEFAULT')
    DB_USER = config.get('SQL', 'DB_USER')
    DB_PASSW = config.get('SQL', 'DB_PASSW')

    # connect to default database
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname={} user={} password={}"
        .format(DB_NAME_DEFAULT, DB_NAME_DEFAULT, DB_PASSW)
        )
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create corpus database
    cur.execute('DROP DATABASE IF EXISTS escorpus')
    cur.execute("CREATE DATABASE escorpus WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()

    # connect to corpus database
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=escorpus user={} password={}"
        .format(DB_USER, DB_PASSW)
        )
    cur = conn.cursor()
    
    return conn, cur

def drop_table(cur, conn, table):
    '''
    Drop a table if it exists'''
    cur.execute(drop_table_query(table))
    conn.commit()

def create_table(cur, conn, table):
    ''' 
    Create empty table
    '''
    if table == 'main_table':
        cur.execute(create_main_table)
    elif table == 'lexicon':
        cur.execute(create_lexicon_table)
    elif table == 'text_source':
        cur.execute(create_source_table)
    else: 
        return
    conn.commit()

def insert_values(cur, conn, table, df):
    ''' 
    insert values from pandas df
    '''
    if table == 'main_table':
        insert_query = insert_main_table
    elif table == 'lexicon':
        insert_query = insert_lexicon_table
    elif table == 'text_source':
        insert_query = insert_source_table
    else: 
        return
    for i, row in df.iterrows():
        cur.execute(insert_query, row.tolist())
        conn.commit()

def main():
    '''
    - (Drops and) creates escorpus database
    - Creates tables 
    - Inserts values into tables from pandas dfs
    - Closes connection to the db
    '''
    conn, cur = create_database()

    for table, df in zip(['main_table', 'lexicon', 'text_source'], 
                         [main_df, lexicon_df, source_df]):
        drop_table(cur, conn, table)
        create_table(cur, conn, table)
        insert_values(cur, conn, table, df)

if __name__ == '__main__':
    main()

