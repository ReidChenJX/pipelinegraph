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
    