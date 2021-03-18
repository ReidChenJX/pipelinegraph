#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2021/3/18 9:56
# @Author   : ReidChen
# Document  ：Will need to preload the data record again, as the parent of the other class.

import pandas as pd
import ahocorasick

class PreData:
    def __init__(self):
        
        # 道路列表与道路actree 树
        self.road_list = [w[0] for w in pd.read_csv('./data/node_road.csv', usecols=[0]).values.tolist()]
        self.road_actree = self.build_actree(self.road_list)
    
        # 水井列表与水井actree 树
        self.manhole_list = [w[0] for w in pd.read_csv('./data/node_manhole_data.csv', usecols=[0]).values.tolist()]
        self.manhole_actree = self.build_actree(self.manhole_list)
    
        # 泵站列表与泵站actree 树
        self.pump_list = [w[0] for w in pd.read_csv('./data/node_pump_data.csv', usecols=[0]).values.tolist()]
        self.pump_actree = self.build_actree(self.pump_list)
    
        # 管道列表与管道actree 树
        self.pipe_list = [w[0] for w in pd.read_csv('./data/node_pipe_data.csv', usecols=[0]).values.tolist()]
        self.pipe_actree = self.build_actree(self.pipe_list)
    
    def build_actree(self, wordlist):
        # 构造 actree，加速过滤
        actree = ahocorasick.Automaton()
        
        for index, word in enumerate(wordlist):
            word = str(word)
            actree.add_word(word, (index, word))
        actree.make_automaton()
        return actree