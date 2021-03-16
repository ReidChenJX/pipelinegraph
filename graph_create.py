#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2021/3/9 10:54
# @Author   : ReidChen
# Document  ：Create node and relationship in graph database.

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
        self.method = 'Create'
        self.__graph = {'profile': "bolt://localhost:7687",
                        'username': "neo4j",
                        'password': "123456cctv@"}
        
        # self.__graph = Graph("bolt://localhost:7687", username="neo4j", password="123456cctv@")
    
    def file_to_node(self):
        # 读取文件，创建实体，实体属性，关系
        pipe_data = pd.read_csv(self.pipe_path, encoding='gb18030')
        pipe_data.drop_duplicates('ps_code2', inplace=True)
        pipe_columns = pipe_data.columns
        pipe_data_ = pipe_data.values
        
        manhole_data = pd.read_csv(self.manhole_path, encoding='gb18030')
        manhole_data.drop_duplicates('ps_code2', inplace=True)
        manhole_columns = manhole_data.columns
        manhole_data_ = manhole_data.values
        
        pump_data = pd.read_csv(self.pump_path, encoding='gb18030')
        pump_data.drop_duplicates('ps_code2', inplace=True)
        pump_columns = pump_data.columns
        pump_data_ = pump_data.values
        
        # 一级实例
        # pipe, manhole, pump = list(),
        # # 二级实例
        # road, in_road, out_road, ac_code = list(), list(), list(), list()
        # 一级实例属性
        pipe_info, manhole_info, pump_info = list(), list(), list()
        
        # 实例关系  需要去重
        pipe_to_road, pipe_to_inroad, pipe_to_outroad, pipe_to_accode = list(), list(), list(), list()
        manhole_to_road, manhole_to_inroad, manhole_to_outroad, manhole_to_accode = list(), list(), list(), list()
        
        for pipe_one in pipe_data_:
            # 字典存储 pipe 实例的属性
            pipe_property = dict()
            cont = 0
            for column in pipe_columns:
                pipe_property[column] = pipe_one[cont]
                
                # 处理 pipe 与其他的关系
                if column == 'road_name':
                    # 增加 pipe_to_road
                    road_name = pipe_one[cont].strip('所属道路：')
                    pipe_to_road.append([pipe_one[0], road_name])
                
                elif column == 'in_roadname':
                    # 增加 pipe_to_inroad
                    inroad = pipe_one[cont].strip('起始道路：')
                    pipe_to_inroad.append([pipe_one[0], inroad])
                
                elif column == 'out_roadname':
                    # 增加 pipe_to_out_roadname
                    outroad = pipe_one[cont].strip('终点道路：')
                    pipe_to_outroad.append([pipe_one[0], outroad])
                
                elif column == 'ad_code':
                    # 增加 pipe_to_ad_code
                    accode = pipe_one[cont].strip()
                    pipe_to_accode.append([pipe_one[0], accode])
                
                cont += 1
            
            pipe_info.append(pipe_property)
        
        for manhole_one in manhole_data_:
            # 字典存储 manhole 实例的属性
            manhole_property = dict()
            cont = 0
            for column in manhole_columns:
                manhole_property[column] = manhole_one[cont]
                
                if column == 'road_name':
                    # 增加 manhole_to_road
                    road_name = manhole_one[cont].strip('所属道路：')
                    manhole_to_road.append([manhole_one[0], road_name])
                
                elif column == 'in_roadname':
                    # 增加 manhole_to_in_roadname
                    inroad = manhole_one[cont].strip('起始道路：')
                    manhole_to_inroad.append([manhole_one[0], inroad])
                
                elif column == 'out_roadname':
                    # 增加 manhole_to_out_roadname
                    outroad = manhole_one[cont].strip('终点道路：')
                    manhole_to_outroad.append([manhole_one[0], outroad])
                
                elif column == 'ad_code':
                    # 增加 manhole_to_ad_code
                    accode = manhole_one[cont].strip()
                    manhole_to_accode.append([manhole_one[0], accode])
                
                cont += 1
            
            manhole_info.append(manhole_property)
        
        for pump_one in pump_data_:
            # 字典存储 pump 实例的属性
            pump_property = dict()
            cont = 0
            for column in pump_columns:
                pump_property[column] = pump_one[cont]
                cont += 1
            
            pump_info.append(pump_property)
        
        # 二级实例：road, in_road, out_road, ac_code
        road_all = pd.concat([pipe_data[['road_name', 'in_roadname', 'out_roadname', 'ad_code']],
                              manhole_data[['road_name', 'in_roadname', 'out_roadname', 'ad_code']]])
        
        road = road_all['road_name'].drop_duplicates().tolist()
        in_road = road_all['in_roadname'].drop_duplicates().tolist()
        out_road = road_all['out_roadname'].drop_duplicates().tolist()
        district = road_all['ad_code'].drop_duplicates().tolist()
        
        # 关系 去重 manhole_to_road, manhole_to_inroad, manhole_to_outroad, manhole_to_accode
        for a_list in [manhole_to_road, manhole_to_inroad, manhole_to_outroad, manhole_to_accode]:
            list_to_pd = pd.DataFrame(a_list)
            list_to_pd.drop_duplicates(inplace=True)
            a_list = list_to_pd.values.tolist()
        
        return pipe_info, manhole_info, pump_info, pipe_to_road, pipe_to_inroad, \
               pipe_to_outroad, pipe_to_accode, road, in_road, out_road, district, \
               manhole_to_road, manhole_to_inroad, manhole_to_outroad, manhole_to_accode
    
    def create_pipe(self, label, node_info, graph):
        # 创建一级节点
        node_nums = len(node_info)
        node_10 = node_nums // 10
        count = 1
        for node_name in node_info:
            node = Node(label, name=node_name['ps_code2'], pipe_level=node_name['pipe_level'],
                        pipe_category=node_name['pipe_category'], pressure_type=node_name['pressure_type'],
                        shape_type=node_name['shapetype'], material=node_name['material'],
                        constr_method=['constr_method'],
                        rconstr_method=node_name['rconstr_method'], road_name=node_name['road_name'],
                        in_roadname=node_name['in_roadname'],
                        out_roadname=node_name['out_roadname'], ad_code=node_name['ad_code'],
                        status=node_name['status'])
            
            graph.create(node)
            count += 1
            if (count / node_10) % 1 == 0:
                print(label + '节点以创建完成：{per} %'.format(per=(count / node_10) * 10))
        
        return
    
    def create_manhole(self, label, node_info, graph):
        # 创建一级节点
        node_nums = len(node_info)
        node_10 = node_nums // 10
        count = 1
        for node_name in node_info:
            node = Node(label, name=node_name['ps_code2'], manhole_type=node_name['manhole_type'],
                        manhole_style=node_name['manhole_style'], cov_dimen1=node_name['cov_dimen1'],
                        surface_elev=node_name['surface_elev'], in_roadname=node_name['in_roadname'],
                        road_name=node_name['road_name'], out_roadname=node_name['out_roadname'],
                        manhole_category=node_name['manhole_category'], ad_code=node_name['ad_code'],
                        junc_class=node_name['junc_class'])
            graph.create(node)
            count += 1
            if (count / node_10) % 1 == 0:
                print(label + '节点以创建完成：{per} %'.format(per=(count / node_10) * 10))
        
        return
    
    def create_pump(self, label, node_info, graph):
        # 创建一级节点
        node_nums = len(node_info)
        node_10 = node_nums // 10
        count = 1
        for node_name in node_info:
            node = Node(label, name=node_name['ps_code2'], ps_category=node_name['ps_category'],
                        ps_category_feat=node_name['ps_category_feat'], status=node_name['status'])
            graph.create(node)
            count += 1
            if (count / node_10) % 1 == 0:
                print(label + '节点以创建完成：{per} %'.format(per=(count / node_10) * 10))
        
        return
    
    def create_sec_node(self, label, nodes, graph):
        # 创建二级节点
        node_nums = len(nodes)
        node_10 = node_nums // 10
        count = 1
        for node_name in nodes:
            node = Node(label, name=node_name)
            graph.create(node)
            count += 1
            if (count / node_10) % 1 == 0:
                print(label + '节点以创建完成：{per} %'.format(per=(count / node_10) * 10))
        
        return
    
    def create_ship(self, relationship, start_node, end_node, rel_type, rel_name, graph):
        # 去重处理后创建关系
        # :param    relationship    关系列表，二维列表
        # :param    start_node  p节点label
        # :param    end_node    q节点label
        # :param    rel_type    关系名称
        # :param    rel_name    关系属性，name的值
        for edge in set(relationship):
            p = edge[0]
            q = edge[1]
            query = "match(p:%s),(q:%s) where p.name='%s'and q.name='%s' create (p)-[rel:%s{name:'%s'}]->(q)" % (
                start_node, end_node, p, q, rel_type, rel_name)
            try:
                graph.run(query)
            except Exception as e:
                print(e)
        return
    
    def create_node_relation(self):
        graph = Graph(self.__graph)
        # 创建实体与关系
        graph.schema.create_uniqueness_constraint(label='pipe', property_key='name')
        graph.schema.create_uniqueness_constraint(label='pump', property_key='name')
        
        pipe_info, manhole_info, pump_info, pipe_to_road, pipe_to_inroad, \
        pipe_to_outroad, pipe_to_accode, road, in_road, out_road, district, \
        manhole_to_road, manhole_to_inroad, manhole_to_outroad, manhole_to_accode = self.file_to_node()
        
        # 创建一级节点 pipe_info, manhole_info, pump_info
        self.create_pipe(label='pipe', node_info=pipe_info, graph=graph)
        self.create_manhole(label='manhole', node_info=manhole_info, graph=graph)
        self.create_pump(label='pump', node_info=pump_info, graph=graph)
        
        # 创建二级节点
        self.create_sec_node(label='road', nodes=road, graph=graph)
        self.create_sec_node(label='in_road', nodes=in_road, graph=graph)
        self.create_sec_node(label='out_road', nodes=out_road, graph=graph)
        self.create_sec_node(label='district', nodes=district, graph=graph)
        
        # 创建节点关系
        self.create_ship(pipe_to_road, 'pipe', 'road', 'belong the way', '所属道路', graph)
        self.create_ship(pipe_to_inroad, 'pipe', 'in_road', 'starting road', '起始道路', graph)
        self.create_ship(pipe_to_outroad, 'pipe', 'out_road', 'end of the road', '终点道路', graph)
        self.create_ship(pipe_to_accode, 'pipe', 'district', 'in district', '所属区', graph)
        
        self.create_ship(manhole_to_road, 'manhole', 'road', 'belong the way', '所属道路', graph)
        self.create_ship(manhole_to_inroad, 'manhole', 'in_road', 'starting road', '起始道路', graph)
        self.create_ship(manhole_to_outroad, 'manhole', 'out_road', 'end of the road', '终点道路', graph)
        self.create_ship(manhole_to_accode, 'manhole', 'district', 'in district', '所属区', graph)


if __name__ == '__main__':
    neo4j_graph = CreateGraph()
    pipe_info, manhole_info, pump_info, pipe_to_road, pipe_to_inroad, \
    pipe_to_outroad, pipe_to_accode, road, in_road, out_road, district, \
    manhole_to_road, manhole_to_inroad, manhole_to_outroad, manhole_to_accode = neo4j_graph.file_to_node()
    neo4j_graph.create_node_relation()
