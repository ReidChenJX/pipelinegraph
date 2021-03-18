#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2021/3/17 17:55
# @Author   : ReidChen
# Document  ：根据转化出来的 result,转化为cypher语句

from question_trans import QueryTrans
import sys
from py2neo import Graph


class SearchAns(QueryTrans):
    
    def __init__(self):
        super(SearchAns, self).__init__()
        
        
    def trans_cypher(self):
        
        pass
        
    def intention_sql(self, que_input):
        
        result = self.means_trans(que_input)
        
        # 解析intention与label
        if 'intentions' not in result:
            print('未能识别提问意图')
            sys.exit()      # 提前结束应用
        intention = result['intentions'][0]
        
        # intention 包含：only_pipe,only_road,only_pump,only_manhole,road relation to manhole
        # road relation to pipe, pipe relation to manhole,manhole relation to pipe
        
        if intention == 'only_pipe':
            label, name = 'pipe',result['road'][0]
            cysql = "MATCH (d:pipe) WHERE d.name='{name}' RETURN d".format(name)
            pass
        
        if intention == 'only_road':
            pass
        
        if intention == 'only_pump':
            pass
        
        if intention == 'only_manhole':
            pass
        
        if intention == 'road relation to manhole':
            pass
        
        if intention == 'road relation to pipe':
            pass
        
        if intention == 'pipe relation to manhole':
            pass
        
        if intention == 'manhole relation to pipe':
            pass
        
            
    
        
# http://172.18.0.201:7474/
# bolt://172.18.0.201:7687

if __name__ == '__main__':
    __graph = {'profile': "http://172.18.0.201:7474/",
               'username': "neo4j",
               'password': "123456cctv@"}
    graph = Graph("http://172.18.0.201:7474/",usename='neo4j',password='123456cctv@')
    
    sqls = "match(n:pipe) where n.name='4030101102010090' return n"
    data = graph.run(sqls).data()
    print(data)
    #
    # sear_ans = SearchAns()
    #
    # ques = input()
    # result = sear_ans.trans_cypher()
    # print(result)

