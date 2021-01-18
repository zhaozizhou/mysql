import pymysql
import logging
import os
import hashlib
#import itertools

#1.创建一个迭代器，拉取主键id。迭代主键ID 每次推进5w
#2.二分法找到有问题的行
#3.抓取列名到列表


master_host = '192.168.1.6'
master_port = 3306
master_user = 'mozis'
master_pass = 'ktlshy34YU$'
master_database_name = 'zzztest'
tablename = 'sbtest10min'
#tablename = 'test50'
slave_host = '192.168.1.6'
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


def row_check(master_host,master_port,master_user,master_pass,master_database_name,tablename,columns):
    list_pri=[]
    list_error=[]
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
    while True:
        list_pri = []
        list_tmp1 = cursor.fetchmany(1000)
        #print(list_pri)
        if list_tmp1 == ():
            break
        for i in list_tmp1:
            list_pri.append(i[0])
        host=master_host
        port=master_port
        user=master_user
        password=master_pass
        database_name=master_database_name
        buchang_in_pri=tuple(list_pri)
        master_md5=md5(database_name,tablename,pri,buchang_in_pri,host,port,user,password,columns)
        host=slave_host
        port=slave_port
        user=slave_user
        password=slave_pass
        database_name=slave_database_name
        slave_md5=md5(database_name,tablename,pri,buchang_in_pri,host,port,user,password,columns)
        #print(master_md5)
        #print(slave_md5)
        if master_md5 == slave_md5:
            #print("{0} check ok!".format(buchang_in_pri))
            logger.debug("{0} to {1} check ok!".format(min(buchang_in_pri),max(buchang_in_pri)))
            #min+=1000
        else:
            list_error.append(buchang_in_pri)
            print("{0} check error!".format(buchang_in_pri))
            #min+=1000
        #if list_tmp1 == ():
           #break
        #print(buchang_in_pri)
    


    #print(type(list_num))
    #print(list_num)
    #pri = "".join('%s' %i for id in list_pri for i in id)
    ##for i in list_tmp1:
    ##    list_pri.append(i[0])
    ##len_list2=len(list_pri)
    ###print(list_pri)  #####主键值
    ##min=0
    ##while True:
    ##    max=min+1000
    ##    if max <= len_list2:
    ##        buchang_in_pri=tuple(list_pri[min:max])
    ##        print(buchang_in_pri)
    ##        host=master_host
    ##        port=master_port
    ##        user=master_user
    ##        password=master_pass
    ##        database_name=master_database_name
    ##        master_md5=md5(database_name,tablename,pri,buchang_in_pri,host,port,user,password,columns)
    ##        host=slave_host
    ##        port=slave_port
    ##        user=slave_user
    ##        password=slave_pass
    ##        database_name=slave_database_name
    ##        slave_md5=md5(database_name,tablename,pri,buchang_in_pri,host,port,user,password,columns)
    ##        #print(master_md5)
    ##        #print(slave_md5)
    ##        if master_md5 == slave_md5:
    ##            #print("{0} check ok!".format(buchang_in_pri))
    ##            logger.debug("{0} check ok!".format(min))
    ##            min+=1000
    ##        else:
    ##            list_error.append(buchang_in_pri)
    ##            print("{0} check error!".format(buchang_in_pri))
    ##            min+=1000
    ##            #break
    ##        #print(min)
    ##    else:
    ##        print('将要填写logging')
    ##        break
    #print(list_pri[min:max])
    #print(len_list2)
    #for x in itertools.count(0,1):
    #    if x < len_list2:
    #        list_num.append(x)
    #    else:
    #        break
    #print(list_num)
    #dic_pri=dict(zip(list_num,list_pri))
    #print(list_pri)
    #print(dic_pri)
    cursor.close() 
    conn.close()
    return(list_error)
    #列名

def column_name(master_host,master_port,master_user,master_pass,master_database_name):
    conn = pymysql.connect(host=master_host, port=int(master_port), user=master_user, passwd=master_pass,db=master_database_name)
    #conn = pymysql.connect(host=master_host, port=int(master_port), user=master_user, passwd=master_pass,db=master_database_name)
    #cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor = conn.cursor()
    cursor.execute("select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='{0}' and TABLE_NAME = '{1}';".format(master_database_name,tablename))
    list_columns = cursor.fetchall()
    columns=",".join('%s' %i for id in list_columns for i in id)
    return(columns)


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
    cursor.execute("select {0} from {1} where {2} in {3}".format(columns,tablename,pri,buchang_in_pri))
    count = cursor.fetchall()
    row = "".join('%s' %i for id in count for i in id)
    row_md5=hashlib.md5(row.encode('utf-8')).hexdigest()
    #print(count)
    #print(row)
    #print(row_md5)
    cursor.close() 
    conn.close()
    return(row_md5)



def main():
    columns=column_name(master_host,master_port,master_user,master_pass,master_database_name)
    #print(columns)
    error=row_check(master_host,master_port,master_user,master_pass,master_database_name,tablename,columns)
    print(error)


if __name__ == "__main__":
    main()