#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2021/3/12 16:28
# @Author   : ReidChen
# Document  ：Import the data to neo4j

from graph_create import CreateGraph
import pandas as pd
import os
import numpy as np

class ImportGraph:
    
    def __init__(self, path=None):
        if path is None:
            cur_dir = '\\'.join(os.path.abspath(__file__).split('\\')[:-1])
        else:
            cur_dir = path
        self.pipe_path = cur_dir + '\\data\\pipe_data.csv'
        self.manhole_path = cur_dir + '\\data\\manhole_data.csv'
        self.pump_path = cur_dir + '\\data\\pump_data.csv'
        self.method = 'Import'
        self.graph = None
    
    def file_to_node(self):
        
        # 一级实例
        pipe_data = pd.read_csv(self.pipe_path, encoding='gb18030')
        pipe_data['ps_code2'] = pipe_data['ps_code2'].apply(lambda x: str(x).strip())
        pipe_data.drop_duplicates('ps_code2', inplace=True)
        pipe_data[':LABEL'] = 'pipe'
    
        manhole_data = pd.read_csv(self.manhole_path, encoding='gb18030')
        manhole_data['ps_code2'] = manhole_data['ps_code2'].apply(lambda x: str(x).strip())
        manhole_data.drop_duplicates('ps_code2', inplace=True)
        manhole_data[':LABEL'] = 'manhole'

        pump_data = pd.read_csv(self.pump_path, encoding='gb18030')
        pump_data['ps_code2'] = pump_data['ps_code2'].apply(lambda x: str(x).strip())
        pump_data.drop_duplicates('ps_code2', inplace=True)
        pump_data[':LABEL'] = 'pump'

        pipe_data.rename({'ps_code2':'name:ID(pipe)'}, axis=1, inplace=True)
        manhole_data.rename({'ps_code2':'name:ID(manhole)'}, axis=1, inplace=True)
        pump_data.rename({'ps_code2':'name:ID(pump)'}, axis=1, inplace=True)
        
        # 二级实例
        road_all = pd.concat([pipe_data[['road_name', 'in_roadname', 'out_roadname', 'ad_code']],
                              manhole_data[['road_name', 'in_roadname', 'out_roadname', 'ad_code']]])

        
        road = road_all['road_name'].apply(lambda x: x.replace('所属道路：','').strip())
        # road = pd.DataFrame(road.values, columns=['name:ID'])
        
        in_road = road_all['in_roadname'].apply(lambda x: x.replace('起始道路：','').strip())
        # in_road = pd.DataFrame(in_road.values, columns=['name:ID'])
        
        out_road = road_all['out_roadname'].apply(lambda x: x.replace('终点道路：','').strip())
        # out_road = pd.DataFrame(out_road.values, columns=['name:ID'])

        # :ID 在neo4j 中有全局唯一性，合并 road
        road = pd.concat([road, in_road, out_road])
        road = pd.DataFrame(road.values, columns=['name:ID(road)'])
        road.drop_duplicates('name:ID(road)',inplace=True)
        road[':LABEL'] = 'road'
        
        district = road_all['ad_code'].drop_duplicates()
        district = pd.DataFrame(district.values, columns=['name:ID(district)'])
        district[':LABEL'] = 'district'
        
        # 构建pipe 与road 的关系

        type, role = 'belong the way', '所属道路'
        be_relation = pipe_data[['name:ID(pipe)', 'road_name']].copy(deep=True)
        be_relation.rename({'name:ID(pipe)': ':START_ID(pipe)', 'road_name': ':END_ID(road)'}, axis=1, inplace=True)
        be_relation[':END_ID(road)'] = be_relation[':END_ID(road)'].apply(lambda x: str(x).replace('所属道路：', '').strip())
        be_relation[[':TYPE', 'role']] = [type, role]

        type, role = 'start the way', '起始道路'
        in_relation = pipe_data[['name:ID(pipe)', 'in_roadname']].copy(deep=True)
        in_relation.rename({'name:ID(pipe)': ':START_ID(pipe)', 'in_roadname': ':END_ID(road)'}, axis=1, inplace=True)
        in_relation[':END_ID(road)'] = in_relation[':END_ID(road)'].apply(lambda x: str(x).replace('起始道路：', '').strip())
        in_relation[[':TYPE', 'role']] = [type, role]

        type, role = 'end the way', '终点道路'
        out_relation = pipe_data[['name:ID(pipe)', 'out_roadname']].copy(deep=True)
        out_relation.rename({'name:ID(pipe)': ':START_ID(pipe)', 'out_roadname': ':END_ID(road)'}, axis=1, inplace=True)
        out_relation[':END_ID(road)'] = out_relation[':END_ID(road)'].apply(lambda x: str(x).replace('终点道路：', '').strip())
        out_relation[[':TYPE', 'role']] = [type, role]

        pipe_to_road_ship = pd.concat([be_relation, in_relation, out_relation])
        
        # 构建 manhole 与 road 的关系
        type, role = 'manhole belong the way', '所属道路'
        be_relation_man = manhole_data[['name:ID(manhole)', 'road_name']].copy(deep=True)
        be_relation_man.rename({'name:ID(manhole)': ':START_ID(manhole)', 'road_name': ':END_ID(road)'}, axis=1, inplace=True)
        be_relation_man[':END_ID(road)'] = be_relation_man[':END_ID(road)'].apply(lambda x: str(x).replace('所属道路：', '').strip())
        be_relation_man[[':TYPE', 'role']] = [type, role]

        type, role = 'manhole start the way', '起始道路'
        in_relation_man = manhole_data[['name:ID(manhole)', 'in_roadname']].copy(deep=True)
        in_relation_man.rename({'name:ID(manhole)': ':START_ID(manhole)', 'in_roadname': ':END_ID(road)'}, axis=1, inplace=True)
        in_relation_man[':END_ID(road)'] = in_relation_man[':END_ID(road)'].apply(lambda x: str(x).replace('起始道路：', '').strip())
        in_relation_man[[':TYPE', 'role']] = [type, role]

        type, role = 'manhole end the way', '终点道路'
        out_relation_man = manhole_data[['name:ID(manhole)', 'out_roadname']].copy(deep=True)
        out_relation_man.rename({'name:ID(manhole)': ':START_ID(manhole)', 'out_roadname': ':END_ID(road)'}, axis=1, inplace=True)
        out_relation_man[':END_ID(road)'] = out_relation_man[':END_ID(road)'].apply(lambda x: str(x).replace('终点道路：', '').strip())
        out_relation_man[[':TYPE', 'role']] = [type, role]

        manhole_to_road_ship = pd.concat([be_relation_man, in_relation_man, out_relation_man])
        
        
        return pipe_data, manhole_data, pump_data, road, district, pipe_to_road_ship, manhole_to_road_ship
    
if __name__ == '__main__':
    import_graph = ImportGraph()
    # 获得实例
    pipe_data, manhole_data, pump_data, road, district, pipe_to_road_ship, manhole_to_road_ship = import_graph.file_to_node()
    
    pipe_data.to_csv('./data/node_pipe_data.csv',index=False)
    
    manhole_data.to_csv('./data/node_manhole_data.csv', index=False)
    
    pump_data.to_csv('./data/node_pump_data.csv',  index=False)
    
    road.to_csv('./data/node_road.csv',  index=False)
    district.to_csv('./data/node_district.csv', index=False)
    pipe_to_road_ship.to_csv('./data/pipe_to_road_ship.csv', index=False)
    manhole_to_road_ship.to_csv('./data/manhole_to_road_ship.csv', index=False)