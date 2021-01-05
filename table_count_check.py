#!/usr/bin/python3

#帮助 --help
#准备文件table_check.conf  可使用 --type=prepare生成模板
#如使用--type=tablecheck 需要 tablename.txt 按照每行一个表名的形式即可
#info文件见 log_info.txt debug文件见log_debug.txt  多次执行请清空

import pymysql
import datetime
import time
import argparse 
import os.path
import configparser
import logging
from multiprocessing import Pool

logger = logging.getLogger(__name__)
logger.setLevel(level = logging.DEBUG)
info = logging.FileHandler("log_info.txt")
info.setLevel(logging.INFO)
formatter_info = logging.Formatter('%(levelname)s - %(message)s')
info.setFormatter(formatter_info)
logger.addHandler(info)


debug = logging.FileHandler("log_debug.txt")
debug.setLevel(logging.DEBUG)
formatter_dbg = logging.Formatter('%(levelname)s - %(message)s')
debug.setFormatter(formatter_dbg)
logger.addHandler(debug)


#try:
#    conf_file_check = open("table_check.conf")             # 返回一个文件对象  
#except IOError as error_file_check:
#    print(error_file_check)


def _argparse():  ##传入参数
    parser = argparse.ArgumentParser(description="数据对比工具v_1.0",add_help=False,)
    parser.add_argument('--type',action='store',choices=['dbcheck','tablecheck','prepare'],default='prepare',dest='_type',required=True,help='dbcheck or tablecheck or prepare ----tablecheck请创建表名文件tablename.txt')
    parser.add_argument('--parallel',action='store',default='1',dest='parallel',help='并发 默认1')
    parser.add_argument('--prepare','-p',action='store_true',dest='prepare',help='准备配置文件')
    parser.add_argument('--help','-h',action="help",help="""#帮助 --help\n
                        准备文件table_check.conf  可使用 --type=prepare生成模板\n
                        如使用--type=tablecheck 需要 tablename.txt 按照每行一个表名的形式即可\n
                        info文件见 log_info.txt debug文件见log_debug.txt  多次执行请清空""")
    return parser.parse_args()

def check_conf_file():  ##配置文件
    conf_file_check = os.path.isfile("table_check.conf")
    if conf_file_check == (True):
        print("table_check.conf --已存在")
        config = configparser.ConfigParser()
        config.read('table_check.conf')
        master_host = config.get('master','master_host')
        master_port = config.get('master','master_port')
        master_user = config.get('master','master_user')
        master_pass = config.get('master','master_pass')
        master_database_name = config.get('master','master_database_name')
        slave_host = config.get('slave','slave_host')
        slave_port = config.get('slave','slave_port')
        slave_user = config.get('slave','slave_user')
        slave_pass = config.get('slave','slave_pass')
        slave_database_name = config.get('slave','slave_database_name')
        return(master_host,master_port,master_user,master_pass,master_database_name,slave_host,slave_port,slave_user,slave_pass,slave_database_name)
    elif conf_file_check == (False):
        config = configparser.ConfigParser()    #类中一个方法 #实例化一个对象
        config["master"] = {'master_host' : '主库IP',
                            'master_port' : '主库端口',
                            'master_user' : '主库用户名',
                            'master_pass' : '密码',
                            'master_database_name' : '数据库名'
                             }	#类似于操作字典的形式
        config["slave"] = {'master_host' : '从库IP',
                            'master_port' : '从库端口',
                            'master_user' : '从库用户名',
                            'master_pass' : '密码',
                            'master_database_name' : '数据库名'
                             }	#类似于操作字典的形式
        #config["slave"] = {'User':'Atlan'} #类似于操作字典的形式
        #config['topsecret.server.com'] = {'Host Port':'50022','ForwardX11':'no'}
        with open('table_check_example.conf', 'w') as configfile:
           config.write(configfile)
        print("配置文件table_check.conf不存在   请按照table_check_example.conf配置")
        return 2


def master(master_host,master_port,master_user,master_pass,master_database_name):  ###连接并检测主库
    conn = pymysql.connect(host=master_host, port=int(master_port), user=master_user, passwd=master_pass,db=master_database_name)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("select table_name from information_schema.tables where table_schema='{0}' and TABLE_TYPE='BASE TABLE'".format(master_database_name))
    table_name_mysql = cursor.fetchall()
    cursor.close() 
    conn.close()
    return table_name_mysql

def slave(slave_host,slave_port,slave_user,slave_pass,slave_database_name):  ###连接并检测从库
    conn = pymysql.connect(host=slave_host, port=int(slave_port), user=slave_user, passwd=slave_pass,db=slave_database_name)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("select table_name from information_schema.tables where table_schema='{0}' and TABLE_TYPE='BASE TABLE'".format(slave_database_name))
    table_name_mysql = cursor.fetchall()
    cursor.close() 
    conn.close()
    return table_name_mysql

def print_error(value,):
    print("error: ", value)


def check_row_num(master_host,master_port,master_user,master_pass,master_database_name,slave_host,slave_port,slave_user,slave_pass,slave_database_name,table_tmp_name):
    #table_tmp_name=i['table_name']
    conn = pymysql.connect(host=master_host, port=int(master_port), user=master_user, passwd=master_pass,db=master_database_name)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    rownum_m=''
    try:
        cursor.execute("select count(*) as rownum from {0}".format(table_tmp_name))
        count = cursor.fetchall()
        cursor.close() 
        conn.close()
        rownum_m=count[0]['rownum']
        logger.debug("MASTER TABLE {0} ROW IS {1}".format(table_tmp_name,rownum_m))
    except pymysql.Error as e:
        print("MASTER {0} ERROR ID {1}  {2}".format(master_host,e.args[0],e.args[1]))
        logger.error("MASTER {0} ERROR ID {1}  {2}".format(master_host,e.args[0],e.args[1]))
    conn = pymysql.connect(host=slave_host, port=int(slave_port), user=slave_user, passwd=slave_pass,db=slave_database_name)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    #cursor.execute("select count(*) as rownum from {0}".format(table_tmp_name))
    #count = cursor.fetchall()
    #cursor.close() 
    #conn.close()
    #rownum_s=count[0]['rownum']
    rownum_s=''
    try:
        cursor.execute("select count(*) as rownum from {0}".format(table_tmp_name))
        count = cursor.fetchall()
        cursor.close() 
        conn.close()
        rownum_s=count[0]['rownum']
        logger.debug("SLAVE TABLE {0} ROW IS {1}".format(table_tmp_name,rownum_s))
    except pymysql.Error as e:
        print("SLAVE {0} ERROR ID {1}  {2}".format(slave_host,e.args[0],e.args[1]))
        logger.error("SLAVE {0} ERROR ID {1}  {2}".format(slave_host,e.args[0],e.args[1]))
    if rownum_m == rownum_s :
        logger.info("CHECK SUCCESS TABLE {0} ROW IS {1}".format(table_tmp_name,rownum_m))
        print("CHECK SUCCESS TABLE {0} ROW IS {1}".format(table_tmp_name,rownum_m))
    else:
        logger.error("MASTER TABLE {0} ROW IS {1} BUT SLAVE TABLE {0} ROW IS {2}----".format(table_tmp_name,rownum_m,rownum_s))
        print("----ERROR ! MASTER TABLE {0} ROW IS {1} BUT SLAVE TABLE {0} ROW IS {2}----".format(table_tmp_name,rownum_m,rownum_s))

def db_check_table(master_host,master_port,master_user,master_pass,master_database_name,slave_host,slave_port,slave_user,slave_pass,slave_database_name):
    list_table_name=[]
    m_table_name=master(master_host,master_port,master_user,master_pass,master_database_name)
    s_table_name=slave(slave_host,slave_port,slave_user,slave_pass,slave_database_name)
    #print(type(m_table_name))
    #print(m_table_name.values())
    for i in m_table_name:
        tolist_table_tmp_name=i['table_name']
        list_table_name.append(tolist_table_tmp_name)
    #print(list_table_name)
    count_m_table=len(m_table_name)
    count_s_table=len(s_table_name)
    return(count_m_table,count_s_table,list_table_name)


def db_check(master_host,master_port,master_user,master_pass,master_database_name,slave_host,slave_port,slave_user,slave_pass,slave_database_name):
    count_m_table=db_check_table(master_host,master_port,master_user,master_pass,master_database_name,slave_host,slave_port,slave_user,slave_pass,slave_database_name)[0]
    count_s_table=db_check_table(master_host,master_port,master_user,master_pass,master_database_name,slave_host,slave_port,slave_user,slave_pass,slave_database_name)[1]
    list_table_name=db_check_table(master_host,master_port,master_user,master_pass,master_database_name,slave_host,slave_port,slave_user,slave_pass,slave_database_name)[2]
    if count_m_table == count_s_table:
        print("---TABLE COUNT CHECK OK ----")
        pool = Pool(processes=parallel)
        for table_tmp_name in list_table_name:
            pool.apply_async(func=check_row_num,args=(master_host,master_port,master_user,master_pass,master_database_name,slave_host,slave_port,slave_user,slave_pass,slave_database_name,table_tmp_name,),error_callback=print_error)
            #check_row_num(master_host,master_port,master_user,master_pass,master_database_name,slave_host,slave_port,slave_user,slave_pass,slave_database_name)
        pool.close()
        pool.join()
    else:
        print("----TABLE COUNT CHECK ERROR----")
        return 1

def table_check(master_host,master_port,master_user,master_pass,master_database_name,slave_host,slave_port,slave_user,slave_pass,slave_database_name):
    list_table_name_tmp=[]
    list_table_name=[]
    try:
        f = open("tablename.txt")             # 返回一个文件对象  
    except IOError as e:
        print(e)
        return 2
    else:
        lines = f.readlines()
        for line in lines:  
            list_table_name_tmp.append(line)                 # 后面跟 ',' 将忽略换行符  
        f.close()  
        list_table_name=[x.strip() for x in list_table_name_tmp]
        #print(list_table_name)
        count_m_table=1
        count_s_table=1
        if count_m_table == count_s_table:
            print("---TABLE COUNT CHECK OK ----")
            pool = Pool(processes=parallel)
            for table_tmp_name in list_table_name:
                pool.apply_async(func=check_row_num,args=(master_host,master_port,master_user,master_pass,master_database_name,slave_host,slave_port,slave_user,slave_pass,slave_database_name,table_tmp_name,),error_callback=print_error)
                #check_row_num(master_host,master_port,master_user,master_pass,master_database_name,slave_host,slave_port,slave_user,slave_pass,slave_database_name)
            pool.close()
            pool.join()
        else:
            print("----TABLE COUNT CHECK ERROR----")
            return 1

def main():
    parser=_argparse()
    _type=parser._type
    global parallel
    parallel=int(parser.parallel)
    global conf_check_conf_file
    conf_check_conf_file=check_conf_file()
    if conf_check_conf_file != 2:
        master_host = conf_check_conf_file[0]
        master_port = conf_check_conf_file[1]
        master_user = conf_check_conf_file[2]
        master_pass = conf_check_conf_file[3]
        master_database_name = conf_check_conf_file[4]
        slave_host = conf_check_conf_file[5]
        slave_port = conf_check_conf_file[6]
        slave_user = conf_check_conf_file[7]
        slave_pass = conf_check_conf_file[8]
        slave_database_name = conf_check_conf_file[9]
    if _type == "dbcheck":
        db_check(master_host,master_port,master_user,master_pass,master_database_name,slave_host,slave_port,slave_user,slave_pass,slave_database_name)
    if _type == "tablecheck":
        table_check(master_host,master_port,master_user,master_pass,master_database_name,slave_host,slave_port,slave_user,slave_pass,slave_database_name)
    if _type == "prepare":
        if conf_check_conf_file != 2:
            print ('''-----------请确认以下参数---------
    '主库IP'        {0}
    '主库端口'      {1}
    '主库用户名'    {2}
    '密码'          {3}
    '主数据库名'    {4}
    '从库IP'        {5}
    '从库端口'      {6}
    '从库用户名'    {7}
    '密码'          {8}
    '从数据库名'    {9}
    '''.format(master_host,master_port,master_user,master_pass,master_database_name,slave_host,slave_port,slave_user,slave_pass,slave_database_name))



if __name__ == "__main__":
    _argparse()
    main()
