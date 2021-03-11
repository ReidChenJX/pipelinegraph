#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2021/3/9 10:53
# @Author   : ReidChen
# Document  ：

import pandas as pd
import numpy as np
import psycopg2


class PostgreConnect:
    def __init__(self):
        self.pipe = './sql/pipe.sql'
        self.manhole = './sql/manhole.sql'
        self.pump = './sql/pump.sql'
    
    def get_pipe(self):
        conn = psycopg2.connect(database="drain_facilities", user="postgres",
                                password="wavenet", host="172.18.0.201", port="5432")
        sql_text = open(file=self.pipe)
        sql = sql_text.readlines()
        sql_text.close()
        sql = ''.join(sql)
        
        pipe_data = pd.read_sql(sql, con=conn)
        
        # rain_data.to_csv('./data/pipe.csv', index=False, encoding='gbk')
        return pipe_data
    
    def get_manhole(self):
        conn = psycopg2.connect(database="drain_facilities", user="postgres",
                                password="wavenet", host="172.18.0.201", port="5432")
        sql_text = open(file=self.manhole)
        sql = sql_text.readlines()
        sql_text.close()
        sql = ''.join(sql)
        
        manhole_data = pd.read_sql(sql, con=conn)
        
        # rain_data.to_csv('./data/pipe.csv', index=False, encoding='gbk')
        return manhole_data
    
    def get_pump(self):
        conn = psycopg2.connect(database="drain_facilities", user="postgres",
                                password="wavenet", host="172.18.0.201", port="5432")
        sql_text = open(file=self.pump)
        sql = sql_text.readlines()
        sql_text.close()
        sql = ''.join(sql)
        
        pump_data = pd.read_sql(sql, con=conn)
        
        # rain_data.to_csv('./data/pipe.csv', index=False, encoding='gbk')
        return pump_data


def dist_change(dist_id):
    if dist_id == 15:
        return '浦东新区'
    elif dist_id == 12:
        return '闵行区'
    elif dist_id == 9:
        return '虹口区'
    elif dist_id == 1:
        return '黄浦区'
    elif dist_id == 16:
        return '金山区'
    elif dist_id == 30:
        return '崇明区'
    elif dist_id == 5:
        return '长宁区'
    elif dist_id == 13:
        return '宝山区'
    elif dist_id == 4:
        return '徐汇区'
    elif dist_id == 17:
        return '松江区'
    elif dist_id == 14:
        return '嘉定区'
    elif dist_id == 7:
        return '普陀区'
    elif dist_id == 10:
        return '杨浦区'
    elif dist_id == 20:
        return '奉贤区'
    elif dist_id == 18:
        return '青浦区'
    elif dist_id == 6:
        return '静安区'
    else:
        return '地区：无'


def pipe_graph(pipe_data):
    def _pipe_level_trans(pipe_level):
        # 1 - 主干管、2 - 次干管（截流管）、3 - 主管、4 - 连管、5 - 接户管、6 - 街坊管、7 - 支管、 8 - 虚拟管道、 9 - 其它
        if pipe_level == 1:
            return '主干管'
        elif pipe_level == 2:
            return '次干管（截流管）'
        elif pipe_level == 3:
            return '主管'
        elif pipe_level == 4:
            return '连管'
        elif pipe_level == 5:
            return '接户管'
        elif pipe_level == 6:
            return '街坊管'
        elif pipe_level == 7:
            return '虚拟管道'
        else:
            return '其他'
    
    def _pipe_cate_trans(pipe_category):
        # 1-雨水、2-污水、3-合流、9-其它
        if pipe_category == 1:
            return '雨水管'
        elif pipe_category == 2:
            return '污水管'
        elif pipe_category == 3:
            return '合流管'
        else:
            '其它'
    
    def _pressure_trans(pressure_type):
        # 管道受到的压力类型：-1-暂缺、0-其他、1-重力、2-压力(E_PRESS)
        pressure_type = int(pressure_type)
        if pressure_type == 1:
            return '管道所受压力：暂缺'
        elif pressure_type == 2:
            return '管道所受压力：其他压力'
        elif pressure_type == 3:
            return '管道所受压力：重力'
        elif pressure_type == 4:
            return '管道所受压力：压力'
        else:
            return '管道所受压力：暂缺'
    
    def _shape_type_trans(shapetype):
        # 1-圆型、2-蛋型、3-矩形、9-其它、901-其它（马蹄形）
        shape_type = int(shapetype)
        if shape_type == 1:
            return '断面：圆型'
        elif shape_type == 2:
            return '断面：蛋型'
        elif shape_type == 3:
            return '断面：矩形'
        elif shape_type == 9:
            return '断面：其它'
        elif shape_type == 901:
            return '断面：其它（马蹄形）'
        else:
            return '断面：暂缺'
    
    def _material_trans(material):
        # 管道材质：1-砼、2-钢砼、3-砖石、4-塑料、401-塑料(PVC)、402-塑料(PE)、403-塑料（PP）、404塑料（玻璃钢）、5-金属管、501-金属管(钢)、502-金属管(铸铁)、9-其它
        if material == 1:
            return '管道材质：砼'
        elif material == 2:
            return '管道材质：钢砼'
        elif material == 3:
            return '管道材质：砖石'
        elif material == 4:
            return '管道材质：塑料'
        elif material == 401:
            return '管道材质：塑料(PVC)'
        elif material == 402:
            return '管道材质：塑料(PE)'
        elif material == 403:
            return '管道材质：塑料（PP）'
        elif material == 404:
            return '管道材质：塑料（玻璃钢）'
        elif material == 5:
            return '管道材质：金属管'
        elif material == 501:
            return '管道材质：金属管(钢)'
        elif material == 502:
            return '管道材质：金属管(铸铁)'
        elif material == 9:
            return '管道材质：其他'
        else:
            return '管道材质：不明确'
    
    def _constr_method_trans(constr_method):
        # 1-开槽埋管、2-顶管、3-盾构、4-拖拉管、9-其它
        constr_method = int(constr_method)
        if constr_method == 1:
            return '管道埋设：开槽埋管'
        elif constr_method == 2:
            return '管道埋设：顶管'
        elif constr_method == 3:
            return '管道埋设：盾构'
        elif constr_method == 4:
            return '管道埋设：拖拉管'
        elif constr_method == 9:
            return '管道埋设：其它'
        else:
            return '管道埋设：不明确'
    
    def _rconstr_method_trans(rconstr_method):
        # 管道修理的方法：-1 - 暂缺、1 - 整体修理（CIPP现场树脂固化、短管内衬、螺旋管内衬）、
        # 2 - 点状修理（涂层法（玻璃钢）、钢套环、节口数量) 3 - 注浆加固(外包环、外套环、其它)
        rconstr_method = int(rconstr_method)
        if rconstr_method == 1:
            return '管道修理方法：整体修理（CIPP现场树脂固化、短管内衬、螺旋管内衬）'
        elif rconstr_method == 2:
            return '管道修理方法：点状修理（涂层法（玻璃钢）、钢套环、节口数量)'
        elif rconstr_method == 3:
            return '管道修理方法：注浆加固(外包环、外套环、其它)'
        else:
            return '管道修理方法：不明确'
    
    def _status_trans(status):
        # 1 - 拟建、2 - 已建、3 - 已废
        status = int(status)
        if status == 1:
            return '设施状态：拟建中'
        elif status == 2:
            return '设施状态：已建'
        elif status == 3:
            return '设施状态：已废'
        else:
            return '设施状态：不明确'
    
    def _road_trans(road):
        if road == 9999:
            return '所属道路：不明'
        else:
            return '所属道路：' + road
    
    def _in_road_trans(in_road):
        if in_road == 9999:
            return '所属道路：不明'
        else:
            return '起始道路：' + in_road
    
    def _out_road_trans(out_road):
        if out_road == 9999:
            return '所属道路：不明'
        else:
            return '终点道路：' + out_road
    
    # 处理pipe 数据，符合neo4j 的录入标准
    pipe_data.replace(to_replace=[None], value=9999, inplace=True)
    pipe_data.fillna(9999, inplace=True)
    
    pipe_data['pipe_level'] = pipe_data['pipe_level'].apply(_pipe_level_trans)
    pipe_data['pipe_category'] = pipe_data['pipe_category'].apply(_pipe_cate_trans)
    pipe_data['pressure_type'] = pipe_data['pressure_type'].apply(_pressure_trans)
    pipe_data['shapetype'] = pipe_data['shapetype'].apply(_shape_type_trans)
    pipe_data['material'] = pipe_data['material'].apply(_material_trans)
    pipe_data['constr_method'] = pipe_data['constr_method'].apply(_constr_method_trans)
    pipe_data['rconstr_method'] = pipe_data['rconstr_method'].apply(_rconstr_method_trans)
    pipe_data['status'] = pipe_data['status'].apply(_status_trans)
    pipe_data['road_name'] = pipe_data['road_name'].apply(_road_trans)
    pipe_data['in_roadname'] = pipe_data['in_roadname'].apply(_in_road_trans)
    pipe_data['out_roadname'] = pipe_data['out_roadname'].apply(_out_road_trans)
    pipe_data['ad_code'] = pipe_data['ad_code'].apply(dist_change)
    
    pipe_tmp = pipe_data[['ps_code2', 'pipe_level', 'pipe_category', 'pressure_type',
                          'shapetype', 'material', 'constr_method', 'rconstr_method', 'status',
                          'road_name', 'in_roadname', 'out_roadname', 'ad_code']]
    
    return pipe_tmp


def manhole_graph(manhole_data):
    def _manhole_type_trans(manhole_type):
        # 0-雨水口、1-检查井、2-接户井、3-闸阀井、4-溢流井、5-倒虹井、6-透气井、7-压力井、8-检测井、9-其它井 、-1-暂缺、10-虚拟井
        manhole_type = int(manhole_type)
        if manhole_type == 0:
            return '雨水口'
        elif manhole_type == 1:
            return '检查井'
        elif manhole_type == 2:
            return '接户井'
        elif manhole_type == 3:
            return '闸阀井'
        elif manhole_type == 4:
            return '溢流井'
        elif manhole_type == 5:
            return '倒虹井'
        elif manhole_type == 6:
            return '透气井'
        elif manhole_type == 7:
            return '压力井'
        elif manhole_type == 8:
            return '检测井'
        elif manhole_type == 9:
            return '其它井'
        elif manhole_type == 10:
            return '虚拟井'
        else:
            return '井的分类：暂缺'
    
    def _manhole_style_trans(manhole_style):
        # 0-一通、1-二通直、2-二通转、3-三通、4-四通、5-五通、6-五通以上、7-暗井、8-侧立型Ⅱ、9-平面型I、10-平面型Ⅲ、11-其它型
        manhole_style = int(manhole_style)
        if manhole_style == 0:
            return '形式：一通'
        elif manhole_style == 1:
            return '形式：二通直'
        elif manhole_style == 2:
            return '形式：二通转'
        elif manhole_style == 3:
            return '形式：三通'
        elif manhole_style == 4:
            return '形式：四通'
        elif manhole_style == 5:
            return '形式：五通'
        elif manhole_style == 6:
            return '形式：五通以上'
        elif manhole_style == 7:
            return '形式：暗井'
        elif manhole_style == 8:
            return '形式：侧立型Ⅱ'
        elif manhole_style == 9:
            return '形式：平面型I'
        elif manhole_style == 10:
            return '形式：平面型Ⅲ'
        elif manhole_style == 11:
            return '形式：其它型'
        else:
            return '形式：不明'
    
    def _cov_dimen1_trans(cov_dimen1):
        # 井盖的材料： -1-暂缺、1-砼、101-砼(铁边)、2-铸铁、3-复合材料、4-塑料、9-其它、901-其它（物业建造）
        cov_dimen1 = int(cov_dimen1)
        if cov_dimen1 == 1:
            return '井盖材料：砼'
        elif cov_dimen1 == 101:
            return '井盖材料：砼(铁边)'
        elif cov_dimen1 == 2:
            return '井盖材料：铸铁'
        elif cov_dimen1 == 3:
            return '井盖材料：复合材料'
        elif cov_dimen1 == 4:
            return '井盖材料：塑料'
        elif cov_dimen1 == 9:
            return '井盖材料：其它'
        elif cov_dimen1 == 901:
            return '井盖材料：其它（物业建造）'
        else:
            return '井盖材料：不明'
    
    def _surface_elev_trans(surface_elev):
        # 1-圆形、2-方形、3-矩形、9-其它
        surface_elev = int(surface_elev)
        if surface_elev == 1:
            return '井盖类型：圆形'
        elif surface_elev == 2:
            return '井盖类型：方形'
        elif surface_elev == 3:
            return '井盖类型：矩形'
        elif surface_elev == 9:
            return '井盖类型：其它'
        else:
            return '井盖类型：不明'
    
    def _junc_class_trans(junc_class):
        # 1-主井、2-附井（接户井）、3-附井（过度井）、4-附井（其它）、5-附井（公共弄堂）、6-附井（非小区管理的）
        junc_class = int(junc_class)
        if junc_class == 1:
            return '主井'
        elif junc_class == 2:
            return '附井（接户井）'
        elif junc_class == 3:
            return '附井（过度井）'
        elif junc_class == 4:
            return '附井（其它）'
        elif junc_class == 5:
            return '附井（公共弄堂）'
        elif junc_class == 6:
            return '附井（非小区管理的）'
        else:
            return '井的类型：不明'
    
    def _manhole_category_trans(manhole_category):
        # 1-雨水井、2-污水井、3-合流井、9-其它
        if manhole_category == 1:
            return '类型：雨水井'
        elif manhole_category == 2:
            return '类型：污水井'
        elif manhole_category == 3:
            return '类型：合流井'
        elif manhole_category == 9:
            return '类型：其它'
        else:
            return '类型：不明'
    
    def _road_trans(road):
        if road == 9999:
            return '所属道路：不明'
        else:
            return '所属道路：' + road
    
    def _in_road_trans(in_road):
        if in_road == 9999:
            return '所属道路：不明'
        else:
            return '起始道路：' + in_road
    
    def _out_road_trans(out_road):
        if out_road == 9999:
            return '所属道路：不明'
        else:
            return '终点道路：' + out_road
    
    # 处理 manhole_data
    manhole_data.replace(to_replace=[None], value=9999, inplace=True)
    manhole_data.fillna(9999, inplace=True)
    
    manhole_data['manhole_type'] = manhole_data['manhole_type'].apply(_manhole_type_trans)
    manhole_data['manhole_style'] = manhole_data['manhole_style'].apply(_manhole_style_trans)
    manhole_data['cov_dimen1'] = manhole_data['cov_dimen1'].apply(_cov_dimen1_trans)
    manhole_data['surface_elev'] = manhole_data['surface_elev'].apply(_surface_elev_trans)
    manhole_data['junc_class'] = manhole_data['junc_class'].apply(_junc_class_trans)
    manhole_data['manhole_category'] = manhole_data['manhole_category'].apply(_manhole_category_trans)
    manhole_data['road_name'] = manhole_data['road_name'].apply(_road_trans)
    manhole_data['in_roadname'] = manhole_data['in_roadname'].apply(_in_road_trans)
    manhole_data['out_roadname'] = manhole_data['out_roadname'].apply(_out_road_trans)
    manhole_data['ad_code'] = manhole_data['ad_code'].apply(dist_change)
    
    return manhole_data


def pump_graph(pump_data):
    def _ps_category_trans(ps_category):
        # 1-雨水、2-污水、3-合建、9-其它
        if ps_category == 1:
            return '雨水泵站'
        elif ps_category == 2:
            return '污水泵站'
        elif ps_category == 3:
            return '合建泵站'
        elif ps_category == 9:
            return '其它泵站'
        else:
            return '未知泵站'
    
    def _ps_category_feat_trans(ps_category_feat):
        # 1-分流雨水、2-合流防汛、3-立交、4-闸泵、5-分流污水、6-合流截流、7-干线输送、8-分流、9-合流、10-其他
        if ps_category_feat == 1:
            return '分类：分流雨水'
        elif ps_category_feat == 2:
            return '分类：合流防汛'
        elif ps_category_feat == 3:
            return '分类：立交'
        elif ps_category_feat == 4:
            return '分类：闸泵'
        elif ps_category_feat == 5:
            return '分类：分流污水'
        elif ps_category_feat == 6:
            return '分类：合流截流'
        elif ps_category_feat == 7:
            return '分类：干线输送'
        elif ps_category_feat == 8:
            return '分类：分流'
        elif ps_category_feat == 9:
            return '分类：合流'
        elif ps_category_feat == 10:
            return '分类：其他'
        else:
            return '分类：未知'
    
    def _status_trans(status):
        # 1-拟建、2-已建、3-已废
        if status == 1:
            return '拟建'
        elif status == 2:
            return '已建'
        elif status == 3:
            return '已废'
    
    pump_data.replace(to_replace=[None], value=9999, inplace=True)
    pump_data.fillna(9999, inplace=True)
    
    pump_data['ps_category'] = pump_data['ps_category'].apply(_ps_category_trans)
    pump_data['ps_category_feat'] = pump_data['ps_category_feat'].apply(_ps_category_feat_trans)
    pump_data['status'] = pump_data['status'].apply(_status_trans)
    
    return pump_data


if __name__ == '__main__':
    # 创建postgresql连接
    postgre_con = PostgreConnect()
    # 处理管道数据
    pipe_data_fir = postgre_con.get_pipe()
    pipe_data = pipe_graph(pipe_data_fir)
    pipe_data.to_csv('./data/pipe_data.csv', index=False, encoding='gb18030')
    # 处理水井数据
    manhole_data_fir = postgre_con.get_manhole()
    manhole_data = manhole_graph(manhole_data_fir)
    manhole_data.to_csv('./data/manhole_data.csv', index=False, encoding='gb18030')
    # 处理泵站数据
    pump_data_fir = postgre_con.get_pump()
    pump_data = pump_graph(pump_data_fir)
    pump_data.to_csv('./data/pump_data.csv', index=False, encoding='gb18030')
