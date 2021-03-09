#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2021/3/9 10:53
# @Author   : ReidChen
# Document  ：

import pandas as pd
import psycopg2


class PostgreConnect:
    def __init__(self):
        self.pipe = './sql/pipe.sql'
        self.manhole = './sql/manhole.sql'
        self.pump = './sql/pump.sql'
        
    def get_pipe(self):
        conn = psycopg2.connect(database="drain_facilities", user="postgres",
                                     password="wavenet", host="172.18.0.201", port="5432")
        sql_text = open(file=self.pipe)
        sql = sql_text.readlines()
        sql_text.close()
        sql = ''.join(sql)
        
        pipe_data = pd.read_sql(sql, con=conn)
        
        # rain_data.to_csv('./data/pipe.csv', index=False, encoding='gbk')
        return pipe_data
    
    
    def get_manhole(self):
        conn = psycopg2.connect(database="drain_facilities", user="postgres",
                                password="wavenet", host="172.18.0.201", port="5432")
        sql_text = open(file=self.manhole)
        sql = sql_text.readlines()
        sql_text.close()
        sql = ''.join(sql)
    
        manhole_data = pd.read_sql(sql, con=conn)
    
        # rain_data.to_csv('./data/pipe.csv', index=False, encoding='gbk')
        return manhole_data

    def get_pump(self):
        conn = psycopg2.connect(database="drain_facilities", user="postgres",
                                password="wavenet", host="172.18.0.201", port="5432")
        sql_text = open(file=self.pump)
        sql = sql_text.readlines()
        sql_text.close()
        sql = ''.join(sql)
    
        pump_data = pd.read_sql(sql, con=conn)
    
        # rain_data.to_csv('./data/pipe.csv', index=False, encoding='gbk')
        return pump_data


def pipe_graph(pipe_data):
    pipe_data = pipe_data
    
    
    pass


if __name__ == '__main__':
    # 创建postgresql连接
    postgre_con = PostgreConnect()

    pipe_data = postgre_con.get_pipe()
    # postgre_con.get_pipe()
    # postgre_con.get_pump()
    
    