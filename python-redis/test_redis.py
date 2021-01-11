import redis
import pymysql
#import sqlite3
from redis.sentinel import Sentinel
#sentinel = Sentinel([('192.168.1.10', 26379)], socket_timeout=0.1)

###声明变量
dic_name_to_sentinel={}

###创建数据库连接
#conn = pymysql.connect(host='192.168.1.10', port=3306, user='mozis', passwd='ktlshy34YU$',db='server_change',charset="utf8")
#cursor = conn.cursor()
# 使用 execute()  方法执行 SQL 查询 
#cursor.execute("select name ,topology,data_ip,business_ip,service_name from redis_sentinel where topology = 'RedisMasterSlave';")
# 使用 fetchone() 方法获取单条数据.
#data = cursor.fetchall()
# 关闭数据库连接
#conn.close()

#取master0  mastername sentinek
def all_sentinel_get():
    #conn = pymysql.connect(host='192.168.1.10', port=3306, user='mozis', passwd='ktlshy34YU$',db='server_change',charset="utf8")
    #cursor = conn.cursor()
    r=redis.Redis(host='192.168.1.10',port=26379)
    #print(r.info(section=None)['master0']['name'])
    all=r.info(section='Sentinel')
    print(r.info(section='Sentinel')['master0'])
    list_master_num=list(r.info(section='Sentinel').keys())
    master_num=list_master_num[4:]
    #print(master_num)
    for num in master_num:
        #print(i)
        name_sentinel=all[num]['name']
        num_sentinel=all[num]['sentinels']
        #print(num_sentinel)
        status_sentinel=all[num]['status']
        slave_sentinel=all[num]['slaves']




def main():
    all_sentinel_get()



if __name__ == "__main__":
    main()