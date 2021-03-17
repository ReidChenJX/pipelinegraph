#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2021/3/12 11:25
# @Author   : ReidChen
# Document  ：Transform the query to sql .


# 输入语句关键词提取，classifier 分类为具体的目的
# 模型构建：特征提取 + word2vec + classifier
# classifier 需构建相识度，选择可能性最大的

import pandas as pd
import unicodedata
import numpy as np
from sklearn.externals import joblib
import jieba
import ahocorasick


def is_number(s):
    # 数值辨析，减少辨析编码时对road产生的误差
    try:
        float(s)
        return True
    except ValueError:
        pass
    
    try:
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass
    
    return False


class QueryTrans:
    '''将输入语句，映射为具体的语义问题'''
    
    def __init__(self):
        # 询问管道信息
        self.pipe_ques = ['管道', '管网', '官网', '观望', '水管', '下水管', '管', 'guan']
        # 询问泵站信息
        self.pump_ques = ['泵站', '排水站', '排水泵站', '泵']
        # 询问水井信息
        self.manhole_ques = ['排水井', '井']
        # 根据道路询问管道
        self.road_pipe = ['有哪些']
        # 根据道路询问水井
        self.road_manhole = []
        # 根据管道询问水井
        self.pipe_manhole = []
        # 根据水井询问管道
        self.manhole_pipe = []
        
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
    
    def model_select(self):
        # 选择模型
        pass
    
    def input_trans(self, input):
    
        # 输入语句语义转化 将输入语句解析为对 道路，官网，水井，泵站的提问句
        self.result = {}
        
        # 判断是否包含对道路, 水井， 泵站的提问
        lu, lu_flag = ['路', '道', '界', '号', '街', '端', '场', '港', '堤', '厂', '村'], 0
        well, well_flag = ['井', '孔'], 0
        ben, ben_flag = ['泵', '站'], 0
        pipe, pipe_flag = ['管','管网'], 0
        
        # 停用词，数据不规范，部分有歧义数据，及时检索出来也不要记录
        lu_stop = ['泵站','水井','管道','检修井']
        
        # 逐字判断是否有满足的关键字
        for word in input:
            if word in lu: lu_flag += 1
            if word in well: well_flag += 1
            if word in ben: ben_flag += 1
        
        # ‘道’与‘’
        message = jieba.lcut(input)
        
        
        
        # 道路精确匹配，可匹配道路列表内的所有道路，但若未完全匹配则不加入result
        if lu_flag > 0:
            for item in self.road_actree.iter(input):
                if 'road' not in self.result and not is_number(item[1][1]) and item[1][1] not in lu_stop:
                    self.result['road'] = [item[1][1]]
                    self.result['road_ind'] = [item[0]]
                elif not is_number(item[1][1]) and item[1][1] not in lu_stop:
                    self.result['road'].append(item[1][1])
                    self.result['road_ind'].append(item[0])
        
        # 道路模糊匹配，采用word2vec 匹配最相近的道路名称
        if lu_flag > 0 and 'road' not in self.result:
            '''  未完待续  '''
            
            pass
        if lu_flag > 0 and 'road' not in self.result:
            print('未匹配到正确道路，请输入正确的道路或场所名称 ！！！')
            
            
        # 水井精确匹配，可匹配水井列表：编号
        if well_flag > 0 :
            for item in self.manhole_actree.iter(input):
                if 'manhole' not in self.result:
                    self.result['manhole'] = [item[1][1]]
                    self.result['manhole_ind'] = [item[0]]
                else:
                    self.result['manhole'].append(item[1][1])
                    self.result['manhole_ind'].append(item[0])
                    
        # 若未能匹配到具体水井，进行提醒
        if well_flag > 0 and 'manhole' not in self.result:
            print('监测到水井关键字，但未匹配到正确编号，请按照水井唯一编号进行搜索 ！！！')
        
        # 泵站精确匹配，按照：编号
        if ben_flag > 0:
            for item in self.pump_actree.iter(input):
                if 'pump' not in self.result:
                    self.result['pump'] = [item[1][1]]
                    self.result['pump_ind'] = [item[0]]
                else:
                    self.result['pump'].append(item[1][1])
                    self.result['pump_ind'].append(item[0])
                    
        # 未能匹配到具体泵站，进行提示
        if ben_flag > 0 and 'pump' not in self.result:
            print('监测到泵站关键字，但未匹配到正确编号，请按照泵站唯一编号进行搜索 ！！！')
        print(self.result)
        
        
        
        
    
    
    
    
    
    def means_trans(self):
        # 语义目的转化
        pass
    
    def query_trans(self):
        # 提问方式转化
        
        pass
    
    def neo4j_anw(self):
        # 在neo4j 中选择答案
        pass


# coding:utf-8
import ahocorasick


def make_AC(AC, word_set):
    for word in word_set:
        AC.add_word(word, word)
    return AC


if __name__ == "__main__":
    query_trans = QueryTrans()
    
    # ques = input('提问：')
    # query_trans.input_trans(ques)
    #
    while True:
        try:
            ques = input('提问：')
            query_trans.input_trans(ques)
        except :
            break
    
    pass
