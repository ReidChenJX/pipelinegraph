#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2021/3/12 11:25
# @Author   : ReidChen
# Document  ：Transform the query to sql .



# 输入语句关键词提取，classifier 分类为具体的目的
# 模型构建：特征提取 + word2vec + classifier
# classifier 需构建相识度，选择可能性最大的

class QueryTrans:
    '''将输入语句，映射为具体的语义问题'''
    def __init__(self):
        # 询问管道信息
        self.pipe_ques= ['管道','管网','官网','观望','水管','下水管','管','guan']
        # 询问泵站信息
        self.pump_ques = ['泵站','排水站','排水泵站','泵']
        # 询问水井信息
        self.manhole_ques = ['排水井','井']
        # 根据道路询问管道
        self.road_pipe = ['有哪些']
        # 根据道路询问水井
        self.road_manhole = []
        # 根据管道询问水井
        self.pipe_manhole = []
        # 根据水井询问管道
        self.manhole_pipe = []
        
        #
        
        
        
        
        pass
    
    def model_select(self):
        # 选择模型
        pass
    
    def input_trans(self):
        # 输入语句语义转化
        pass
        
    def means_trans(self):
        # 语义目的转化
        pass
    
    def query_trans(self):
        # 提问方式转化
        pass
    
    def neo4j_anw(self):
        # 在neo4j 中选择答案
        pass
    
    
#coding:utf-8
import ahocorasick

def make_AC(AC, word_set):
    for word in word_set:
        AC.add_word(word,word)
    return AC

def test_ahocorasick():
    '''
    ahocosick：自动机的意思
    可实现自动批量匹配字符串的作用，即可一次返回该条字符串中命中的所有关键词
    '''
    key_list = ["苹果", "香蕉", "梨", "橙子", "柚子", "火龙果", "柿子", "猕猴挑"]
    AC_KEY = ahocorasick.Automaton()
    AC_KEY = make_AC(AC_KEY, set(key_list))
    AC_KEY.make_automaton()
    test_str_list = ["我最喜欢吃的水果有：苹果、梨和香蕉", "我也喜欢吃香蕉，但是我不喜欢吃梨"]
    for content in test_str_list:
        name_list = set()
        for item in AC_KEY.iter(content):#将AC_KEY中的每一项与content内容作对比，若匹配则返回
            name_list.add(item[1])
        name_list = list(name_list)
        if len(name_list) > 0:
            print(content, "--->命中的关键词有：", "\t".join(name_list))
if __name__ == "__main__":
    test_ahocorasick()
