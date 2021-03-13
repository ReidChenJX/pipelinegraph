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
        pipe_data[':LABEL'] = 'pipe'
    
        manhole_data = pd.read_csv(self.manhole_path, encoding='gb18030')
        manhole_data.drop_duplicates('ps_code2', inplace=True)
        manhole_data[':LABEL'] = 'manhole'

        pump_data = pd.read_csv(self.pump_path, encoding='gb18030')
        pump_data.drop_duplicates('ps_code2', inplace=True)
        pump_data[':LABEL'] = 'pump'

        pipe_data.rename({'ps_code2':'name:ID'}, axis=1, inplace=True)
        manhole_data.rename({'ps_code2':'name:ID'}, axis=1, inplace=True)
        pump_data.rename({'ps_code2':'name:ID'}, axis=1, inplace=True)
        
        # 二级实例
        road_all = pd.concat([pipe_data[['road_name', 'in_roadname', 'out_roadname', 'ad_code']],
                              manhole_data[['road_name', 'in_roadname', 'out_roadname', 'ad_code']]])

        
        road = road_all['road_name'].apply(lambda x: x.replace('所属道路：',''))
        # road = pd.DataFrame(road.values, columns=['name:ID'])
        
        in_road = road_all['in_roadname'].apply(lambda x: x.replace('起始道路：',''))
        # in_road = pd.DataFrame(in_road.values, columns=['name:ID'])
        
        out_road = road_all['out_roadname'].apply(lambda x: x.replace('终点道路：',''))
        # out_road = pd.DataFrame(out_road.values, columns=['name:ID'])

        # :ID 在neo4j 中有全局唯一性，合并 road
        road = pd.concat([road, in_road, out_road])
        road = pd.DataFrame(road.values, columns=['name:ID'])
        road[':LABEL'] = 'road'
        
        district = road_all['ad_code'].drop_duplicates()
        district = pd.DataFrame(district.values, columns=['name:ID'])
        district[':LABEL'] = 'district'
        
        # 构建pipe 与road 的关系

        type, role = 'belong the way', '所属道路'
        be_relation = pipe_data[['name:ID', 'road_name']].copy(deep=True)
        be_relation.rename({'name:ID': ':START_ID', 'road_name': ':END_ID'}, axis=1, inplace=True)
        be_relation[':END_ID'] = be_relation[':END_ID'].apply(lambda x: x.replace('所属道路：', ''))
        be_relation[[':TYPE', 'role']] = [type, role]

        type, role = 'start the way', '起始道路'
        in_relation = pipe_data[['name:ID', 'in_roadname']].copy(deep=True)
        in_relation.rename({'name:ID': ':START_ID', 'in_roadname': ':END_ID'}, axis=1, inplace=True)
        in_relation[':END_ID'] = in_relation[':END_ID'].apply(lambda x: x.replace('起始道路：', ''))
        in_relation[[':TYPE', 'role']] = [type, role]

        type, role = 'end the way', '终点道路'
        out_relation = pipe_data[['name:ID', 'out_roadname']].copy(deep=True)
        out_relation.rename({'name:ID': ':START_ID', 'out_roadname': ':END_ID'}, axis=1, inplace=True)
        out_relation[':END_ID'] = out_relation[':END_ID'].apply(lambda x: x.replace('终点道路：', ''))
        out_relation[[':TYPE', 'role']] = [type, role]

        pipe_to_road_ship = pd.concat([be_relation, in_relation, out_relation])
        
        # 构建 manhole 与 road 的关系
        type, role = 'manhole belong the way', '所属道路'
        be_relation_man = manhole_data[['name:ID', 'road_name']].copy(deep=True)
        be_relation_man.rename({'name:ID': ':START_ID', 'road_name': ':END_ID'}, axis=1, inplace=True)
        be_relation_man[':END_ID'] = be_relation_man[':END_ID'].apply(lambda x: x.replace('所属道路：', ''))
        be_relation_man[[':TYPE', 'role']] = [type, role]

        type, role = 'manhole start the way', '起始道路'
        in_relation_man = manhole_data[['name:ID', 'in_roadname']].copy(deep=True)
        in_relation_man.rename({'name:ID': ':START_ID', 'in_roadname': ':END_ID'}, axis=1, inplace=True)
        in_relation_man[':END_ID'] = in_relation_man[':END_ID'].apply(lambda x: x.replace('起始道路：', ''))
        in_relation_man[[':TYPE', 'role']] = [type, role]

        type, role = 'manhole end the way', '终点道路'
        out_relation_man = manhole_data[['name:ID', 'out_roadname']].copy(deep=True)
        out_relation_man.rename({'name:ID': ':START_ID', 'out_roadname': ':END_ID'}, axis=1, inplace=True)
        out_relation_man[':END_ID'] = out_relation_man[':END_ID'].apply(lambda x: x.replace('终点道路：', ''))
        out_relation_man[[':TYPE', 'role']] = [type, role]

        manhole_to_road_ship = pd.concat([be_relation_man, in_relation_man, out_relation_man])
        
        
        return pipe_data, manhole_data, pump_data, road, district, pipe_to_road_ship, manhole_to_road_ship
    
    

    
    
    


if __name__ == '__main__':
    import_graph = ImportGraph()
    # 获得实例
    pipe_data, manhole_data, pump_data, road, district, pipe_to_road_ship, manhole_to_road_ship = import_graph.file_to_node()
    
    pipe_data.to_csv('./data/node_pipe_data.csv',encoding='gb18030',index=False)
    manhole_data.to_csv('./data/node_manhole_data.csv', encoding='gb18030', index=False)
    pump_data.to_csv('./data/node_pump_data.csv', encoding='gb18030', index=False)
    road.to_csv('./data/node_road.csv', encoding='gb18030', index=False)
    district.to_csv('./data/node_district.csv', encoding='gb18030', index=False)
    pipe_to_road_ship.to_csv('./data/pipe_to_road_ship.csv', encoding='gb18030', index=False)
    manhole_to_road_ship.to_csv('./data/manhole_to_road_ship.csv', encoding='gb18030', index=False)


    
    
    

    