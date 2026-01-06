#!/usr/bin/python3
"""
@Author ：kehongping
@Date   ：2024/4/30 10:31
@Desc   ：This is a tool for import excel/csv to mysql
"""
import os
import argparse
import sys
import time
import csv
import traceback
import logging

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


def parse_options():
    parser = argparse.ArgumentParser(description='This is a tool for import excel/csv to mysql')
    parser.add_argument('-H', '--host', type=str, dest="host", required=False, default='127.0.0.1',
                        help="default mysql host: 127.0.0.1")
    parser.add_argument('-P', '--port', type=str, dest="port", required=False, default='3306',
                        help="default mysql port 3306")
    parser.add_argument('-u', '--user', type=str, dest="user", required=False, default='root',
                        help="default mysql user root")
    parser.add_argument('-p', '--password', type=str, dest="password", required=False, default='123456',
                        help="default mysql password 123456")
    parser.add_argument('-d', '--db', type=str, dest="db", required=True, default='', help="mysql db")
    parser.add_argument('-t', '--table', type=str, required=True, dest='table', help="mysql table")
    parser.add_argument('-f', '--file', type=str, dest='file', required=True, help="path to excel/csv file")
    parser.add_argument('-e', '--encoding', type=str, dest='encoding', required=False, default='utf-8',
                        help="default file encoding utf-8")
    args = parser.parse_args()

    return args


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


def connect_to_mysql(host: str, port: int, user: str, password: str, db: str):
    """
    连接mysql数据库
    :param host:
    :param port:
    :param user:
    :param password:
    :param db:
    :return:
    """
    try:
        conn = pymysql.connect(
            host=host,
            port=int(port),
            user=user,
            password=password,
            database=db,
            autocommit=True
        )
        logger.info("Successfully connected to MySQL database")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to MySQL database: {e}")
        return None


def batch_insert_data(cursor, table: str, data_list: list):
    """
    批量插入数据
    """
    start_time = time.time()
    columns = ', '.join(data_list[0].keys())
    placeholders = ', '.join(['%s'] * len(data_list[0]))
    sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
    values = [tuple(d.values()) for d in data_list]
    cursor.executemany(sql, values)
    end_time = time.time()
    logger.info(f"Import data length:{len(data_list)}, cost: {round(end_time - start_time, 2)}s")


def data_insert_mysql(data_generator, host: str, port: int, user: str, password: str, db: str, table: str,
                      batch_size=10000):
    """
    将数据批量插入mysql
    """
    conn = connect_to_mysql(host, port, user, password, db)
    if conn is None:
        return

    try:
        cursor = conn.cursor()
        count = 0
        data_list = []
        for data in data_generator:
            count += 1
            data_list.append(data)
            if len(data_list) == batch_size:
                batch_insert_data(cursor, table, data_list)
                data_list = []

        if data_list:  # 处理剩余数据
            batch_insert_data(cursor, table, data_list)

        logger.info(f"Data total: {count}, inserted successfully into MySQL table")
    except Exception as e:
        logger.error(f"Error inserting data into MySQL table: {traceback.format_exc()}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    args = parse_options()
    start_time = time.time()

    file_extension = os.path.splitext(args.file)[1]

    if file_extension in ('.xls', '.xlsx'):
        data_insert_mysql(xls_generator_data(args.file), args.host, args.port, args.user, args.password, args.db,
                          args.table)
    elif file_extension in ('.csv', ):
        data_insert_mysql(csv_generator_data(args.file, args.encoding), args.host, args.port, args.user, args.password,
                          args.db,
                          args.table)
    else:
        logger.error('The file format is not supported, only excel/csv formats are supported')
        sys.exit(1)

    end_time = time.time()
    logger.info(f"Import finish, cost time: {round(end_time - start_time, 2)}s")
