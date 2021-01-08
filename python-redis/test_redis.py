import redis
from redis.sentinel import Sentinel
#sentinel = Sentinel([('192.168.1.10', 26379)], socket_timeout=0.1)
r=redis.Redis(host='192.168.1.10',port=26379)
print(r.info(section=None)['master0']['name'])
print(r.info(section='Sentinel')['master0']['sentinels'])
list_master_num=list(r.info(section='Sentinel').keys())
print(list_master_num[4:])

