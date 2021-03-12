#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2021/3/12 16:28
# @Author   : ReidChen
# Document  ：Import the data to neo4j

from graph_create import CreateGraph
import pandas as pd
import numpy as np

class ImportGraph(CreateGraph):
    
    def __init__(self, path=None):
        super(ImportGraph, self ).__init__(path)
        self.method = 'Import'
        
    
    def file_to_node(self):
        
        # 一级实例
        pipe_data = pd.read_csv(self.pipe_path, encoding='gb18030')
        pipe_data.drop_duplicates('ps_code2', inplace=True)
        pipe_data.apply(lambda x:"\"{x}\"".format(x=x))
    
        manhole_data = pd.read_csv(self.manhole_path, encoding='gb18030')
        manhole_data.drop_duplicates('ps_code2', inplace=True)

        pump_data = pd.read_csv(self.pump_path, encoding='gb18030')
        pump_data.drop_duplicates('ps_code2', inplace=True)

        pipe_data.rename({'ps_code2':'name:ID'}, axis=1, inplace=True)
        manhole_data.rename({'ps_code2':'name:ID'}, axis=1, inplace=True)
        
        # 二级实例
        road_all = pd.concat([pipe_data[['road_name', 'in_roadname', 'out_roadname', 'ad_code']],
                              manhole_data[['road_name', 'in_roadname', 'out_roadname', 'ad_code']]])

        road = road_all['road_name'].drop_duplicates()
        in_road = road_all['in_roadname'].drop_duplicates()
        out_road = road_all['out_roadname'].drop_duplicates()
        district = road_all['ad_code'].drop_duplicates()

        district = district.rename({'ac_code':'district:ID'})
        

        
        
    
    
    pass


if __name__ == '__main__':
    import_graph = ImportGraph()
    a = import_graph.pump_path
    
    print(a)
    