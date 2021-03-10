#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2021/3/9 10:54
# @Author   : ReidChen
# Document  ：

from py2neo import Graph,Schema, Node, Relationship
import numpy as np
import pandas as pd
import re
import os


class CreateGraph:
    def __init__(self, path=None):
        if path == None:
            cur_dir = '\\'.join(os.path.abspath(__file__).split('\\')[:-1])
        else:
            cur_dir = path
        self.pipe_path = cur_dir + '\\data\\pipe_data.csv'
        self.manhole_path = cur_dir + '\\data\\manhole_data.csv'
        self.pump_path = cur_dir + '\\data\\pump_data.csv'
        
        self.graph = Graph("bolt://localhost:7687", username="neo4j", password="123456cctv@")
        
        
    def file_to_node(self):
        # 读取文件，创建实体，实体属性，关系
        pipe_data = pd.read_csv(path=self.pipe_path, encoding='gb18030')
        manhole_data = pd.read_csv(path=self.manhole_path, encoding='gb18030')
        pump_data = pd.read_csv(path=self.pump_path, encoding='gb18030')



    
    def create_node(self):
        # 创建实体
        self.graph.schema.create_uniqueness_constraint(label='Person', property_key='name')
        pass
    
    def create_ship(self):
        # 创建关系
        pass
    





if __name__ == '__main__':
    url = 'http://localhost:7474/browser/'
    graph = Graph(url, username='neo4j', password='123456cctv@')

    a = Node("Person", name="Alice")
    b = Node("Person", name="Bob")
    c = Node("Person", name="Carol")
    KNOWS = Relationship.type("KNOWS")
    ab = KNOWS(a, b)
    ba = KNOWS(b, a)
    ac = KNOWS(a, c)
    ca = KNOWS(c, a)
    bc = KNOWS(b, c)
    cb = KNOWS(c, b)
    friends = ab | ba | ac | ca | bc | cb
    g = graph
    g.schema.create_uniqueness_constraint(label='Person',property_key='name')
    g.create(friends)
    