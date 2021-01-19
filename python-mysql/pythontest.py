#list1=((40517,), (27054,), (56027,), (49089,), (13214,), (61550,), (8217,), (30485,), (56735,), (65932,))
#list2=[]
#for i in list1: list2.append(i[0])
#print(list2)
#
#
##print(list1)


#import itertools
#list_pri=[42, 13, 50, 48, 12, 22, 37, 15, 45, 18, 47, 17, 28, 31, 49, 41, 32, 25, 1, 14, 5, 19, 23, 8, 21, 6, 36, 46, 20, 30, 7, 34, 24, 2, 4, 29, 35, 43, 44, 10, 40, 3, 9, 27, 11, 26, 38, 33, 39, 16]
#min=0
#max=min+10
#print(list_pri[min:max])


#import pymysql
#list_pri=[]
#conn = pymysql.connect(host='192.168.1.6', port=3306, user='mozis', passwd='ktlshy34YU$',db='zzztest',charset="utf8")
#cursor = conn.cursor()
#cursor.execute("select id from sbtest10;")
##count = cursor.fetchall()
##count = cursor.fetchmany(5)
#while True:
#    count = cursor.fetchmany(2)
#    list_pri=[]
#    for i in count:
#        list_pri.append(i[0])
#    if count == ():
#        break
#    print(list_pri)
#tuple_list_pri=tuple(list_pri)
##print(tuple_list_pri)
#conn.close()

import random
import time
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

parallel=1
def a(p):
    p=p+1
    print("a =={0}".format(p))
    return(p)


def b(d):
    print("b =={0}".format(d))

def print_error(value,):
    print("error: ", value)

if __name__=="__main__":
    pool = Pool(processes=2)
    for i in range(10):
        zz=random.randrange(10)
        print(zz)
        pool.apply_async(func=a,args=(zz,),callback=b,error_callback=print_error)
        #pool.apply_async(func=b,args=(q,),error_callback=print_error)
        time.sleep(2)
        #check_row_num(master_host,master_port,master_user,master_pass,master_database_name,slave_host,slave_port,slave_user,slave_pass,slave_database_name)
    pool.close()
    pool.join()