import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import json


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('conn done')


    load_staging_tables(cur, conn)
    print('loadTables done')

    insert_tables(cur, conn)
    print('inserttables -ALL- done')


    conn.close()


if __name__ == "__main__":
    main()
