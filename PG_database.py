#!/usr/bin python
# coding=utf-8

"""
@Create_date: 2017-08-01
@Author: tangcheng
@Email: tangcheng@cstech.ltd
@description: 操作数据库
Copyright (c) 2017 HangZhou CSTech.Ltd. All rights reserved.
"""

import logging
import psycopg2
import psycopg2.extras

import config

def sql_query(sql, binds=()):
    """
    执行一条SQL语句
    :param sql: 字符串类型, 有绑定变量的SQL语句，如select * from table tablename where col1=%s and col2=%s;
    :param binds: tuple数组类型，代表传进来的各个绑定变量
    :return: 返回查询到的结果集，是一个数组，数组中每个元素可以看做是一个数组，也可以看做是一个字典
    """
    db_name = config.get('db_name')
    db_host = config.get('db_host')
    db_port = config.get('db_port')
    db_user = config.get('db_user')
    db_pass = config.get("db_pass")
    conn = psycopg2.connect(database=db_name, user=db_user, password=db_pass, host=db_host, port=db_port)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    logging.debug("Run SQL: %s" % sql)
    cur.execute(sql, binds)
    msg = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    return msg


def sql_query_dict(sql, binds=()):
    """
    执行一条SQL语句
    :param sql: 字符串类型, 有绑定变量的SQL语句
    :param binds: tuple数组类型，代表传进来的各个绑定变量
    :return: 返回查询到的结果集，是一个数组，数组中每个元素是一个真正的字典
    """

    db_name = config.get('db_name')
    db_host = config.get('db_host')
    db_port = config.get('db_port')
    db_user = config.get('db_user')
    db_pass = config.get("db_pass")
    conn = psycopg2.connect(database=db_name, user=db_user, password=db_pass, host=db_host, port=db_port)

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    logging.debug("Run SQL: %s" % sql)
    cur.execute(sql, binds)
    msg = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    return msg


def sql_exec(sql, binds=()):
    """
    执行一条无需要结果的SQL语句，通常是DML语句
    :param sql: 字符串类型, 有绑定变量的SQL语句
    :param binds: tuple数组类型，代表传进来的各个绑定变量
    :return: 无
    """

    db_name = config.get('db_name')
    db_host = config.get('db_host')
    db_port = config.get('db_port')
    db_user = config.get('db_user')
    db_pass = config.get("db_pass")
    conn = psycopg2.connect(database=db_name, user=db_user, password=db_pass, host=db_host, port=db_port)

    cur = conn.cursor()
    logging.debug("Run SQL: %s" % sql)
    cur.execute(sql, binds)
    conn.commit()
    cur.close()
    conn.close()
