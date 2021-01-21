import pymysql
import logging
import os
import hashlib
#import itertools
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import redis
from redis.sentinel import Sentinel



#redis
sentinel = Sentinel([('192.168.1.10', 26379),('192.168.1.10', 26380),('192.168.1.10', 26381)],socket_timeout=0.5)
master = sentinel.discover_master('mymaster')
master = sentinel.master_for('mymaster',socket_timeout=0.5,db=0,encoding='utf-8',decode_responses=True)#

#1.创建一个迭代器，拉取主键id。迭代主键ID 每次推进5w
#2.二分法找到有问题的行
#3.抓取列名到列表

parallel=5
parallel_table_long=10
master_host = '192.168.1.6'
master_port = 3306
master_user = 'mozis'
master_pass = 'ktlshy34YU$'
master_database_name = 'zzztest'
#tablename = 'sbtest10'
#tablename = 'test50'
slave_host = '192.168.21.128'
slave_port = 3306
slave_user = 'mozis'
slave_pass = 'ktlshy34YU$'
slave_database_name = 'zzztest'


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


##表名
def table_name_list():
    list_table_name_tmp=[]
    list_table_name=[]
    try:
        f = open("tablename.txt")             # 返回一个文件对象  
    except IOError as e:
        #print(e)
        return(e)
    else:
        lines = f.readlines()
        for line in lines:  
            list_table_name_tmp.append(line)                 # 后面跟 ',' 将忽略换行符  
        f.close()  
        list_table_name=[x.strip() for x in list_table_name_tmp]
        return(list_table_name)

##取列名
def column_name(master_host,master_port,master_user,master_pass,master_database_name,tablename):
    conn = pymysql.connect(host=master_host, port=int(master_port), user=master_user, passwd=master_pass,db=master_database_name)
    #conn = pymysql.connect(host=master_host, port=int(master_port), user=master_user, passwd=master_pass,db=master_database_name)
    #cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor = conn.cursor()
    cursor.execute("select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='{0}' and TABLE_NAME = '{1}';".format(master_database_name,tablename))
    list_columns = cursor.fetchall()
    columns=",".join('%s' %i for id in list_columns for i in id)
    return(columns)

def md5_check(master_database_name,tablename,pri,master_host,master_port,master_user,master_pass,columns,slave_database_name,slave_host,slave_port,slave_user,slave_pass,list_tmp1,buchang_in_pri):
    list_pri=[]
    list_error=[]
    if len(buchang_in_pri) == 0:
        #print(list_pri)
        for i in list_tmp1:
            list_pri.append(i[0])
        buchang_in_pri_tmp=tuple(list_pri)
        buchang_in_pri=(",".join('%s' %id for id in list_pri))
        #print(buchang_in_pri)
        master_md5=md5(master_database_name,tablename,pri,buchang_in_pri,master_host,master_port,master_user,master_pass,columns)
        slave_md5=md5(slave_database_name,tablename,pri,buchang_in_pri,slave_host,slave_port,slave_user,slave_pass,columns)
        #print(master_md5)
        #print(slave_md5)
        if master_md5 == slave_md5:
            #print("{0} check ok!".format(buchang_in_pri))
            logger.debug("TABLE {0} IS CHECKING".format(tablename))
            #min+=1000
        else:
            list_error.append(buchang_in_pri_tmp)
            for ide in buchang_in_pri_tmp:
                #print(ide)
                master.sadd(tablename,ide)
            logger.debug("TABLE {0} in {1} check error!".format(tablename,buchang_in_pri))
            #print("TABLE {0} in {1} check error!".format(tablename,buchang_in_pri))
        if len(list_error) == 0:
            logger.info("TABLE {0} CHECK OK !".format(tablename))
            #return()
        else:
            return(list_error)
    else:
        #buchang_in_pri=list_tmp1
        master_md5=md5(master_database_name,tablename,pri,buchang_in_pri,master_host,master_port,master_user,master_pass,columns)
        slave_md5=md5(slave_database_name,tablename,pri,buchang_in_pri,slave_host,slave_port,slave_user,slave_pass,columns)
        if master_md5 == slave_md5:
            #print("{0} check ok!".format(buchang_in_pri))
            logger.debug("TABLE {0} IS CHECKING".format(tablename))
            #min+=1000
        else:
            list_error.append(buchang_in_pri)
            #for ide in buchang_in_pri_tmp:
                #print(ide)
                #master.sadd(tablename,ide)
            logger.error("TABLE {0}  {1} check error!".format(tablename,buchang_in_pri))
            print("TABLE {0}  {1} check error!".format(tablename,buchang_in_pri))
        if len(list_error) == 0:
            logger.info("TABLE {0} CHECK OK !".format(tablename))
            #return()
        else:
            return(list_error)



#遍历主键取xxxx行带入MD5
def row_check_1111(master_host,master_port,master_user,master_pass,master_database_name,tablename):
    #pool = Pool(processes=10)
    #columns=column_name(master_host,master_port,master_user,master_pass,master_database_name,tablename)
    #print(columns)
    #list_pri=[]
    #dic_pri={}
    conn = pymysql.connect(host=master_host, port=int(master_port), user=master_user, passwd=master_pass,db=master_database_name)
    #cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor = conn.cursor()
    #主键名
    cursor.execute("select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='{0}' and TABLE_NAME='{1}' and COLUMN_KEY='PRI';".format(master_database_name,tablename))
    list_pri_name = cursor.fetchall()
    pri = "".join('%s' %i for id in list_pri_name for i in id) ###主键名字
    #print(pri)
    #主键值
    cursor.execute("select {0} from {1} ".format(pri,tablename)) ########limit1
    #list_tmp1 = cursor.fetchall()  ###未处理的主键值

def row_check(master_host,master_port,master_user,master_pass,master_database_name,tablename,table_check_open,list_tmp1):
    pri=pri_list(master_host,master_port,master_user,master_pass,master_database_name,tablename)
    columns=column_name(master_host,master_port,master_user,master_pass,master_database_name,tablename)
    if table_check_open == 0:
        buchang_in_pri=()
        conn = pymysql.connect(host=master_host, port=int(master_port), user=master_user, passwd=master_pass,db=master_database_name)
        #cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor = conn.cursor()
        cursor.execute("select {0} from {1} ".format(pri,tablename))
        tpool = ThreadPool(10)# 创建一个线程池，20个线程数
        while True:
            #list_pri = []
            list_tmp2 = cursor.fetchmany(parallel_table_long) #######mysql取多少数据
            list_tmp1=list(filter(None,list_tmp2))
            #print(type(list_tmp2))
            #list_tmp1 = cursor.fetchone(1)
            #print(list_tmp2)
            if list_tmp2 == ():
                break
            tpool.apply_async(func=md5_check,args=(master_database_name,tablename,pri,master_host,master_port,master_user,master_pass,columns,slave_database_name,slave_host,slave_port,slave_user,slave_pass,list_tmp1,buchang_in_pri,),error_callback=print_error)
        tpool.close()
        tpool.join()
        cursor.close() 
        conn.close()
    elif table_check_open == 1:
        #conn = pymysql.connect(host=master_host, port=int(master_port), user=master_user, passwd=master_pass,db=master_database_name)
        #cursor = conn.cursor(pymysql.cursors.DictCursor)
        #cursor = conn.cursor()
        #cursor.execute("select {0} from {1} ".format(pri,tablename))
        tpool = ThreadPool(10)# 创建一个线程池，20个线程数
        list_tmp2=list_tmp1
        for list_tmp1 in list_tmp2:
        #while True:
            #list_pri = []
            #list_tmp2 = cursor.fetchmany(parallel_table_long) #######mysql取多少数据
            #list_tmp1=list(filter(None,list_tmp2))
            #print(type(list_tmp2))
            #list_tmp1 = cursor.fetchone(1)
            #print(list_tmp2)
            #print(list_tmp1)
            buchang_in_pri=list_tmp1
            tpool.apply_async(func=md5_check,args=(master_database_name,tablename,pri,master_host,master_port,master_user,master_pass,columns,slave_database_name,slave_host,slave_port,slave_user,slave_pass,list_tmp1,buchang_in_pri,),error_callback=print_error)
        tpool.close()
        tpool.join()
        #cursor.close() 
        #conn.close()
        #print("debug22222")
    #print("{0} check done".format(tablename))
    #列名
    return("{0} check done".format(tablename))
    #return

#校验表名
def check_table(master_database_name,tablename,master_host,master_port,master_user,master_pass,slave_database_name,slave_host,slave_port,slave_user,slave_pass):
    conn = pymysql.connect(host=master_host, port=int(master_port), user=master_user, passwd=master_pass,db=master_database_name)
    #cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor = conn.cursor()
    try:
        # 执行sql语句
        cursor.execute("select 1 from {0} limit 1;".format(tablename))
    # except:
    except pymysql.Error as e:
        return(e.args[0], e.args[1])
        # print(e[0])
    conn = pymysql.connect(host=slave_host, port=int(slave_port), user=slave_user, passwd=slave_pass,db=slave_database_name)
    #cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor = conn.cursor()
    try:
        # 执行sql语句
        cursor.execute("select 1 from {0} limit 1;".format(tablename))
    # except:
    except pymysql.Error as e:
        return(e.args[0], e.args[1])
        # print(e[0])
    #parallel_table_long=10
    table_check_open=0
    list_tmp1=[]
    #row_check_tmp=row_check(master_host,master_port,master_user,master_pass,master_database_name,tablename,table_check_open,list_tmp1)
    row_check(master_host,master_port,master_user,master_pass,master_database_name,tablename,table_check_open,list_tmp1)
    #return(row_check_tmp)
    return()



#计算md5
def md5(database_name,tablename,pri,buchang_in_pri,host,port,user,password,columns):
    conn = pymysql.connect(host=host, port=int(port), user=user, passwd=password,db=database_name)
    #conn = pymysql.connect(host=master_host, port=int(master_port), user=master_user, passwd=master_pass,db=master_database_name)
    #cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor = conn.cursor()
    #cursor.execute("select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='{0}' and TABLE_NAME = '{1}';".format(database_name,tablename))
    #list_columns = cursor.fetchall()
    #columns=",".join('%s' %i for id in list_columns for i in id)
    #print(columns)
    #行数据
    #for iii in list_pri:
    #print(buchang_in_pri)
    cursor.execute("select {0} from {1} where {2} in ({3})".format(columns,tablename,pri,buchang_in_pri))
    count = cursor.fetchall()
    row = "".join('%s' %i for id in count for i in id)
    row_md5=hashlib.md5(row.encode('utf-8')).hexdigest()
    #print(count)
    #print(row)
    #print(row_md5)
    cursor.close() 
    conn.close()
    return(row_md5)


def pri_list(master_host,master_port,master_user,master_pass,master_database_name,tablename):
    #pool = Pool(processes=10)
    #columns=column_name(master_host,master_port,master_user,master_pass,master_database_name,tablename)
    #print(columns)
    #list_pri=[]
    #dic_pri={}
    conn = pymysql.connect(host=master_host, port=int(master_port), user=master_user, passwd=master_pass,db=master_database_name)
    #cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor = conn.cursor()
    #主键名
    cursor.execute("select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='{0}' and TABLE_NAME='{1}' and COLUMN_KEY='PRI';".format(master_database_name,tablename))
    list_pri_name = cursor.fetchall()
    pri = "".join('%s' %i for id in list_pri_name for i in id) ###主键名字
    return(pri)



def last_check():
    list_table_name=table_name_list()
    pool = Pool(processes=parallel)
    for tablename in list_table_name:
        count_error=master.scard(tablename)
        if count_error == 0:
            continue
        else:
            #parallel_table_long=1
            table_check_open=1
            list_tmp1=list(master.smembers(tablename))
            #print(list_tmp1)
            #pri=pri_list(master_host,master_port,master_user,master_pass,master_database_name,tablename)
            #columns=column_name(master_host,master_port,master_user,master_pass,master_database_name,tablename)
            pool.apply_async(func=row_check,args=(master_host,master_port,master_user,master_pass,master_database_name,tablename,table_check_open,list_tmp1,),callback=print_ture,error_callback=print_error)
            #print("last_check done")
    pool.close()
    pool.join()
    #return("last_check done")

def print_error(value,):
    print("error: ", value)

def print_ture(value,):
    print(value)

def main():
    table_names=table_name_list()
    #print(table_names)
    pool = Pool(processes=parallel)
    for tablename in table_names:
        pool.apply_async(func=check_table,args=(master_database_name,tablename,master_host,master_port,master_user,master_pass,slave_database_name,slave_host,slave_port,slave_user,slave_pass,),error_callback=print_error)
            #pool.apply_async(func=row_check,args=(master_host,master_port,master_user,master_pass,master_database_name,tablename,),callback=print_ture,error_callback=print_error)
    #check_row_num(master_host,master_port,master_user,master_pass,master_database_name,slave_host,slave_port,slave_user,slave_pass,slave_database_name)
    pool.close()
    pool.join()

    last_check()



if __name__ == "__main__":
    main()
