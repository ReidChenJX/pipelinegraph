#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2021/3/9 10:54
# @Author   : ReidChen
# Document  ：

from py2neo import Graph, Node, Relationship
import numpy as np
import pandas as pd
import re
import os


class CreateGraph:
    def __init__(self, path=None):
        if path is None:
            cur_dir = '\\'.join(os.path.abspath(__file__).split('\\')[:-1])
        else:
            cur_dir = path
        self.pipe_path = cur_dir + '\\data\\pipe_data.csv'
        self.manhole_path = cur_dir + '\\data\\manhole_data.csv'
        self.pump_path = cur_dir + '\\data\\pump_data.csv'
        
        self.graph = Graph("bolt://localhost:7687", username="neo4j", password="123456cctv@")
    
    def file_to_node(self):
        # 读取文件，创建实体，实体属性，关系
        pipe_data = pd.read_csv(self.pipe_path, encoding='gb18030')
        pipe_columns = pipe_data.columns
        pipe_data = pipe_data.values
        
        manhole_data = pd.read_csv(self.manhole_path, encoding='gb18030')
        manhole_columns = manhole_data.columns
        manhole_data = manhole_data.values
        
        pump_data = pd.read_csv(self.pump_path, encoding='gb18030')
        pump_columns = pump_data.columns
        pump_data = pump_data.values
        
        # 一级实例
        pipe, manhole, pump = list(), list(), list()
        # 二级实例
        in_road, out_road, ac_code = list(), list(), list()
        # 一级实例属性
        pipe_info, manhole_info, pump_info = list(), list(), list()
        
        # 实例关系
        pipe_to_road, pipe_to_inroad, pipe_to_outroad, pipe_to_accode = list(), list() ,list(), list()
        
        for pipe_one in pipe_data:
            # 字典存储 pipe 实例的属性
            pipe_property = dict()
            cont = 0
            for column in pipe_columns:
                pipe_property[column], = pipe_one[cont]
                
                # 处理 pipe 与其他的关系
                if column == 'road_name':
                    # 增加 pipe_to_road
                    road_name = pipe_one[cont].strip('所属道路：')
                    pipe_to_road.append([pipe_one[0], road_name])

                if column == 'in_roadname':
                    # 增加 pipe_to_inroad
                    inroad = pipe_one[cont].strip('起始道路：')
                    pipe_to_inroad.append([pipe_one[0], inroad])
                    
                if column == 'out_roadname':
                    # 增加 pipe_to_out_roadname
                    outroad = pipe_one[cont].strip('终点道路：')
                    pipe_to_outroad.append([pipe_one[0], outroad])
                    
                if column == 'ad_code':
                    # 增加 pipe_to_ad_code
                    accode = pipe_one[cont].strip()
                    pipe_to_accode.append([pipe_one[0], accode])
                    
                cont += 1
            
            pipe_info.append(pipe_property)
        
        for manhole_one in manhole_data:
            # 字典存储 manhole 实例的属性
            manhole_property = dict()
            cont = 0
            for column in manhole_columns:
                manhole_property[column] = manhole_one[cont]
                
                if column == 'road_name':
                    # 增加 manhole_to_road
                    pass
                if column == 'in_roadname':
                    # 增加 manhole_to_in_roadname
                    pass
                if column == 'out_roadname':
                    # 增加 manhole_to_out_roadname
                    pass
                if column == 'ad_code':
                    # 增加 manhole_to_ad_code
                    pass
                
                cont += 1

            manhole_info.append(manhole_property)
        
        
        for pump_one in pump_data:
            # 字典存储 pump 实例的属性
            pump_property = dict()
            cont = 0
            for column in pump_columns:
                pump_property[column] = pump_one[cont]
                cont += 1
            
            pump_info.append(pump_property)
        
        
        return
        
    
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
    g.schema.create_uniqueness_constraint(label='Person', property_key='name')
    g.create(friends)
