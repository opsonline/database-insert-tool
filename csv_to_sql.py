#!/usr/bin/python3
"""
@Author ：kehongping
@Date   ：2024/4/30 10:31
@Desc   ：This is a tool for import excel/csv to mysql
"""
import datetime
import os
import argparse
import sys
import time
import csv
import traceback
import logging
import re
import xlrd
import pymysql


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s: %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


logger = get_logger('import_data')


def csv_generator_data(path: str, encoding='utf-8'):
    """
    生成器函数，用于逐行读取 CSV 文件中的数据。
    Args:
        path (str): CSV 文件路径。
        encoding (str, optional): 文件编码格式，默认为 'utf-8'。

    Yields:
        dict: 包含每行数据的字典。

    Raises:
        FileNotFoundError: 如果指定路径的文件不存在。
        UnicodeDecodeError: 如果文件解码失败。

    Example:
        示例用法：

        >>> for row in csv_generator_data('data.csv'):
        ...     print(row)
        {'column1': 'value1', 'column2': 'value2', ...}
        {'column1': 'value3', 'column2': 'value4', ...}
    """
    with open(path, 'r', encoding=encoding) as file:
        reader = csv.DictReader(file)
        for row in reader:
            yield row


def xls_generator_data(path: str):
    """
    生成器函数，用于逐行读取 XLS 文件中的数据。
    Args:
        path (str): XLS 文件路径。

    Yields:
        dict: 包含每行数据的字典。

    Raises:
        FileNotFoundError: 如果指定路径的文件不存在。
        XLRDError: 如果无法解析 XLS 文件。

    Example:
        示例用法：

        >>> for row in xls_generator_data('data.xls'):
        ...     print(row)
        {'column1': 'value1', 'column2': 'value2', ...}
        {'column1': 'value3', 'column2': 'value4', ...
    """
    wb = xlrd.open_workbook(path)
    sheet = wb.sheets()[0]
    titles = sheet.row_values(0)

    for i in range(1, sheet.nrows):
        the_row_data = []
        for j in range(sheet.ncols):
            cell_type = sheet.cell(i, j).ctype  # 表格的数据类型
            # 判断python读取的返回类型  0 --empty,1 --string, 2 --number(都是浮点), 3 --date, 4 --boolean, 5 --error
            cell = sheet.cell_value(i, j)
            #
            if cell_type == 2 and cell % 1 == 0.0:
                cell = int(cell)  # 浮点转成整型

            cell = str(cell).strip()

            the_row_data.append(cell)

            row_dict = dict(zip(titles, the_row_data))

        yield row_dict


def csv_to_update_sql(table, where_key, set_dict: dict):
    """
    :param table:
    :param where_key:
    :param set_dict:
    :return:
    """
    set_key_list = [f"{k}='{v}'" for k, v in set_dict.items()]

    set_str = ','.join(set_key_list)
    sql = f"update {table} set {set_str} where {where_key};"
    print(sql)


class StrClearRule(object):
    def __init__(self, str):
        self.str = str if str else ''

    def rule_1(self):
        if re.search(r"&middot;", self.str):
            return self.str.replace("&middot;", "·")
        return self.str

    def rule_2(self):
        if re.search(r" ", self.str):
            return self.str.replace(" ", "")
        return self.str

    def rule_3(self):
        if re.search(r'\(.*\)', self.str):
            return re.sub(r'\(.*\)', "", self.str)
        return self.str

    def rule_4(self):
        if re.search(r'&[a-zA-Z]+;', self.str):
            return re.sub(r'&[a-zA-Z]+;', "", self.str)
        return self.str

    def rule_5(self):
        if re.search(r'[，。‘’‘\-\|？↙/↗↘`、Ⅲ！／〉￥＼∵√\\]+', self.str):
            return re.sub(r'[，。‘’‘\-\|？↙/↗↘`、Ⅲ！／〉￥＼∵√\\]+', "", self.str)
        return self.str

    def rule_6(self):
        if re.search(" ", self.str):
            return self.str.replace(" ", "")
        return self.str

    def rule_7(self):
        if re.search(r"[×Ｘｘ×㐅✕✘Χ✖️❌☓✗✘卍]+", self.str):
            return re.sub(r'[×Ｘｘ×㐅✕✘Χ✖️❌☓✗✘卍]+', "X", self.str)
        return self.str

    def rule_8(self):
        return self.rule_2()

    def rule_9(self):
        if re.search("`", self.str):
            return self.str.replace("`", "")
        return self.str

def clear_str(str, rule_name):
    clear_engine = StrClearRule(str)
    clear_rule = getattr(clear_engine, rule_name)
    if not clear_rule:
        raise RuntimeError(f'{rule_name}规则不存在')

    return clear_rule()


def order_business_sql(order_business_files: list):
    for index, f in enumerate(order_business_files):
        rows = csv_generator_data(f)
        update_time = f'2024-07-20 01:00:{index}'
        for row in rows:
            if row.get('投保人姓名') or row.get('被报人姓名'):
                # 先执行规则二
                pro_insure_name_2 = row['投保人姓名'].replace(" ", "")
                pro_the_insure_name_2 = row['被报人姓名'].replace(" ", "")

                # 规则1
                pro_insure_name_1 = pro_insure_name_2.replace("&middot;", "·")
                pro_the_insure_name_1 = pro_the_insure_name_2.replace("&middot;", "·")

                # 规则3
                pro_insure_name_3_1 = re.sub(r'\(.*\)', "", pro_insure_name_1)
                pro_the_insure_name_3_1 = re.sub(r'\(.*\)', "", pro_the_insure_name_1)
                # pro_insure_name_3_2 = re.sub(r'\（.*\）', "", pro_insure_name_3_1)
                # pro_the_insure_name_3_2 = re.sub(r'\（.*\）', "", pro_the_insure_name_3_1)

                # 规则4
                pro_insure_name_4 = re.sub(r'&[a-zA-Z]+;', "", pro_insure_name_3_1)
                pro_the_insure_name_4 = re.sub(r'&[a-zA-Z]+;', "", pro_the_insure_name_3_1)

                # 规则5
                pro_insure_name_5 = re.sub(r'[，。‘’‘\-\|？↙/↗↘`、Ⅲ！／〉￥＼∵√\\]+', "", pro_insure_name_4)
                pro_the_insure_name_5 = re.sub(r'[，。‘’‘\-\|？↙/↗↘`、Ⅲ！／〉￥＼∵√\\]+', "", pro_the_insure_name_4)

                csv_to_update_sql('order_business', f"business_id={row['business_id']}",
                                  {'pro_insure_name': pro_insure_name_5,
                                   'pro_the_insure_name': pro_the_insure_name_5, 'update_time': update_time})

            elif row.get('投保人证件号') or row.get('被报人证件号'):

                # 规则6
                pro_insure_id_6 = row['投保人证件号'].replace(" ", "")
                pro_the_insure_id_6 = row['被报人证件号'].replace(" ", "")

                # 规则7
                pro_insure_id_7 = re.sub(r'[×Ｘｘ×㐅✕✘Χ✖️❌☓✗✘卍]+', "X", pro_insure_id_6)
                pro_the_insure_id_7 = re.sub(r'[×Ｘｘ×㐅✕✘Χ✖️❌☓✗✘卍]+', "X", pro_the_insure_id_6)

                csv_to_update_sql('order_business', f"business_id={row['business_id']}",
                                  {'pro_insure_id': pro_insure_id_6,
                                   'pro_the_insure_id': pro_the_insure_id_7, 'update_time': update_time})

            elif row.get('手机号'):
                # 规则8
                pro_insure_phone_8 = row['手机号'].replace(" ", "")

                # 规则8
                pro_insure_phone_9 = pro_insure_phone_8.replace("`", "")
                csv_to_update_sql('order_business', f"business_id={row['business_id']}",
                                  {'pro_insure_phone': pro_insure_phone_9, 'update_time': update_time})


def order_business_sql_v2(file_name, rule_name, update_time):
    rows = csv_generator_data(file_name)

    for row in rows:
        if row.get('投保人姓名') or row.get('被报人姓名'):
            csv_to_update_sql('order_business', f"business_id={row['business_id']}",
                                  {'pro_insure_name': clear_str(row['投保人姓名'], rule_name), 'pro_the_insure_name': clear_str(row['被报人姓名'], rule_name),
                                   'update_time': update_time})

        if row.get('投保人证件号') or row.get('被报人证件号'):
            csv_to_update_sql('order_business', f"business_id={row['business_id']}",
                                  {'pro_insure_id': clear_str(row['投保人证件号'], rule_name), 'pro_the_insure_id': clear_str(row['被报人证件号'], rule_name), 'update_time': update_time})

        if row.get('手机号'):
            csv_to_update_sql('order_business', f"business_id={row['business_id']}",
                                  {'pro_insure_phone': clear_str(row['手机号'], rule_name), 'update_time': update_time})


def order_business_details_sql_v2(file_name, rule_name, update_time):
    rows = csv_generator_data(file_name)

    for row in rows:
        if row.get('投保人姓名') or row.get('被报人姓名'):
            csv_to_update_sql('order_business_details', f"id={row['id']}",
                                  {'pro_insure_name': clear_str(row['投保人姓名'], rule_name), 'pro_the_insure_name': clear_str(row['被报人姓名'], rule_name),
                                   'update_time': update_time})

        if row.get('投保人证件号') or row.get('被报人证件号'):
            csv_to_update_sql('order_business_details', f"id={row['id']}",
                                  {'pro_insure_cert_no': clear_str(row['投保人证件号'], rule_name), 'pro_the_insure_cert_no': clear_str(row['被报人证件号'], rule_name), 'update_time': update_time})

        if row.get('手机号'):
            csv_to_update_sql('order_business_details', f"id={row['id']}",
                                  {'pro_insure_phone': clear_str(row['手机号'], rule_name), 'update_time': update_time})


def order_member_the_insure_v2(file_name, rule_name, update_time):
    rows = csv_generator_data(file_name)

    for row in rows:
        if row.get('学生姓名') or row.get('被报人姓名'):
            csv_to_update_sql('order_member_the_insure', f"id={row['id']}",
                              {'student_name': clear_str(row['学生姓名'], rule_name),
                               'pro_the_insure_name': clear_str(row['被报人姓名'], rule_name), 'car_id_code': update_time})

        if row.get('学生证件号') or row.get('被报人证件号'):
            csv_to_update_sql('order_member_the_insure', f"id={row['id']}",
                              {'student_cert_no': clear_str(row['学生证件号'], rule_name),
                               'pro_the_insure_cert_no': clear_str(row['被报人证件号'], rule_name), 'car_id_code': update_time})

        if row.get('投保人手机号') or row.get('被保人手机号'):
            csv_to_update_sql('order_member_the_insure', f"id={row['id']}",
                              {'pro_insure_phone': clear_str(row['投保人手机号'], rule_name),
                               'pro_the_insure_phone': clear_str(row['被保人手机号'], rule_name), 'car_id_code': update_time})


def order_business_details_sql(order_business_details_files: list, rule):
    for index, f in enumerate(order_business_details_files):
        rows = csv_generator_data(f)

        base_time = 1721408400000
        for row in rows:
            if row.get('投保人姓名') or row.get('被报人姓名'):
                # 先执行规则二
                pro_insure_namp_2 = row['投保人姓名'].replace(" ", "")
                pro_the_insure_name_2 = row['被报人姓名'].replace(" ", "")

                # 规则1
                pro_insure_name_1 = pro_insure_namp_2.replace("&middot;", "·")
                pro_the_insure_name_1 = pro_the_insure_name_2.replace("&middot;", "·")

                # 规则3
                pro_insure_name_3_1 = re.sub(r'\(.*\)', "", pro_insure_name_1)
                pro_the_insure_name_3_1 = re.sub(r'\(.*\)', "", pro_the_insure_name_1)
                # pro_insure_name_3_2 = re.sub(r'\（.*\）', "", pro_insure_name_3_1)
                # pro_the_insure_name_3_2 = re.sub(r'\（.*\）', "", pro_the_insure_name_3_1)

                # 规则4
                pro_insure_name_4 = re.sub(r'&[a-zA-Z]+;', "", pro_insure_name_3_1)
                pro_the_insure_name_4 = re.sub(r'&[a-zA-Z]+;', "", pro_the_insure_name_3_1)

                # 规则5
                pro_insure_name_5 = re.sub(r'[，。‘’‘\-\|？↙/↗↘`、Ⅲ！／〉￥＼∵√\\]+', "", pro_insure_name_4)
                pro_the_insure_name_5 = re.sub(r'[，。‘’‘\-\|？↙/↗↘`、Ⅲ！／〉￥＼∵√\\]+', "", pro_the_insure_name_4)

                csv_to_update_sql('order_business_details', f"id={row['id']}",
                                  {'pro_insure_name': pro_insure_name_5,
                                   'pro_the_insure_name': pro_the_insure_name_5,
                                   'update_time': int(1721408400000 + index)})

            elif row.get('投保人证件号') or row.get('被报人证件号'):

                # 规则6
                pro_insure_id_6 = row['投保人证件号'].replace(" ", "")
                pro_the_insure_id_6 = row['被报人证件号'].replace(" ", "")

                # 规则7
                pro_insure_id_7 = re.sub(r'[×Ｘｘ×㐅✕✘Χ✖️❌☓✗✘卍]+', "X", pro_insure_id_6)
                pro_the_insure_id_7 = re.sub(r'[×Ｘｘ×㐅✕✘Χ✖️❌☓✗✘卍]+', "X", pro_the_insure_id_6)

                csv_to_update_sql('order_business_details', f"id={row['id']}",
                                  {'pro_insure_cert_no': pro_insure_id_6,
                                   'pro_the_insure_cert_no': pro_the_insure_id_7,
                                   'update_time': int(1721408400000 + index)})

            elif row.get('手机号'):
                # 规则8
                pro_insure_phone_8 = row['手机号'].replace(" ", "")

                # 规则8
                pro_insure_phone_9 = pro_insure_phone_8.replace("`", "")
                csv_to_update_sql('order_business_details', f"id={row['id']}",
                                  {'pro_insure_phone': pro_insure_phone_9, 'update_time': int(1721408400000 + index)})


def order_member_the_insure_sql(order_member_the_insure_files: list):
    for index, f in enumerate(order_member_the_insure_files):
        rows = csv_generator_data(f)

        car_id_code = f'2024-07-20 01:00:{index}'
        for row in rows:
            if row.get('学生姓名', '') or row.get('被报人姓名', ''):
                student_name_0 = row['学生姓名'] if row.get('学生姓名', '') else ''
                pro_the_insure_name_0 = row['被报人姓名'] if row.get('被报人姓名', '') else ''

                # 先执行规则二
                student_name_2 = student_name_0.replace(" ", "")
                pro_the_insure_name_2 = pro_the_insure_name_0.replace(" ", "")

                # 规则1
                student_name_1 = student_name_2.replace("&middot;", "·")
                pro_the_insure_name_1 = pro_the_insure_name_2.replace("&middot;", "·")

                # 规则3
                student_name_3_1 = re.sub(r'\(.*\)', "", student_name_1)
                pro_the_insure_name_3_1 = re.sub(r'\(.*\)', "", pro_the_insure_name_1)
                # student_name_3_2 = re.sub(r'\（.*\）', "", student_name_3_1)
                # pro_the_insure_name_3_2 = re.sub(r'\（.*\）', "", pro_the_insure_name_3_1)

                # 规则4
                student_name_4 = re.sub(r'&[a-zA-Z]+;', "", student_name_3_1)
                pro_the_insure_name_4 = re.sub(r'&[a-zA-Z]+;', "", pro_the_insure_name_3_1)

                # 规则5
                student_name_5 = re.sub(r'[，。‘’‘\-\|？↙/↗↘`、Ⅲ！／〉￥＼∵√\\]+', "", student_name_4)
                pro_the_insure_name_5 = re.sub(r'[，。‘’‘\-\|？↙/↗↘`、Ⅲ！／〉￥＼∵√\\]+', "", pro_the_insure_name_4)

                csv_to_update_sql('order_member_the_insure', f"id={row['id']}",
                                  {'student_name': student_name_5,
                                   'pro_the_insure_name': pro_the_insure_name_5, 'car_id_code': car_id_code})


            elif row.get('学生证件号') or row.get('被报人证件号'):

                # 规则6
                student_cert_6 = row['学生证件号'].replace(" ", "")
                pro_the_insure_id_6 = row['被报人证件号'].replace(" ", "")

                # 规则7
                student_cert_7 = re.sub(r'[×Ｘｘ×㐅✕✘Χ✖️❌☓✗✘卍]+', "X", student_cert_6)
                pro_the_insure_id_7 = re.sub(r'[×Ｘｘ×㐅✕✘Χ✖️❌☓✗✘卍]+', "X", pro_the_insure_id_6)

                csv_to_update_sql('order_member_the_insure', f"id={row['id']}",
                                  {'student_cert': student_cert_7,
                                   'pro_the_insure_cert_no': pro_the_insure_id_7, 'car_id_code': car_id_code})

            elif row.get('投保人手机号') or row.get('被保人手机号'):
                # 规则8
                pro_insure_phone_8 = row['投保人手机号'].replace(" ", "")
                pro_the_insure_phone_8 = row['被保人手机号'].replace(" ", "")

                # 规则8
                pro_insure_phone_9 = pro_insure_phone_8.replace("`", "")
                pro_the_insure_phone_9 = pro_the_insure_phone_8.replace("`", "")
                csv_to_update_sql('order_member_the_insure', f"id={row['id']}",
                                  {'pro_insure_phone': pro_insure_phone_9,
                                   'pro_the_insure_phone': pro_the_insure_phone_9, 'car_id_code': car_id_code})


if __name__ == "__main__":
    order_business_files = [
        "规则1_order_business.csv",
        "规则2_order_business.csv",
        "规则3_order_business.csv",
        "规则4_order_business.csv",
        "规则5_order_business.csv",
        "规则6_order_business.csv",
        "规则7_order_business.csv",
        "规则8_order_business.csv",
        "规则9_order_business.csv"
    ]

    order_business_details_files = [
        "规则1_order_business_details.csv",
        "规则2_order_business_details.csv",
        "规则3_order_business_details.csv",
        "规则4_order_business_details.csv",
        "规则5_order_business_details.csv",
        "规则6_order_business_details.csv",
        "规则7_order_business_details.csv",
        "规则8_order_business_details.csv",
        "规则9_order_business_details.csv"
    ]

    order_member_the_insure_files = [
        "规则1_order_insure_memeber.csv",
        "规则2_order_insure_memeber.csv",
        "规则3_order_insure_memeber.csv",
        "规则4_order_insure_memeber.csv",
        "规则6_order_insure_member.csv",
        "规则5_order_insure_memeber.csv",
        "规则7_order_insure_memeber.csv",
        "规则8_order_insure_memeber.csv",
        "规则9_order_insure_member.csv"
    ]

    #    order_business_sql(order_business_files)

    #    order_business_details_sql(order_business_details_files)

    #    order_member_the_insure_sql(order_member_the_insure_files)
    # order_business_sql_v2(sys.argv[1], sys.argv[2], sys.argv[3])
    # order_business_details_sql_v2(sys.argv[1], sys.argv[2], sys.argv[3])
    order_member_the_insure_v2(sys.argv[1], sys.argv[2], sys.argv[3])

