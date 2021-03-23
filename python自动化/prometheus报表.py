# -*- coding:utf-8 -*- #
 
import requests 
import time
 
'''
采样范围太大就会提示
{"status":"error","errorType":"execution",
"error":"query processing would load too many samples into memory in query execution"}
'''
 
check_address = {
                "test1":"http://192.168.21.128:9090/"
                }
                 
                 
check_target= {"总使用率":"node_filesystem_size_bytes{instance='Linux_21.128'}"}
 
for project,address in check_address.items():
    print("---------------项目{}---------------".format(address))
               
    for key,expr in check_target.items():
        print("[{}]".format(key))
        url = address + '/api/v1/query?query=' + expr
        print(url)
        response = requests.get(url,params={'query':'expr'})
         
        try:
            for i in response.json()['data']['result']:
                check_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(i['value'][0]))
                print("实例{}\t采集时间{}\t当前数值{}".format(i['metric']['instance'],check_time,i['value'][1]))
        except Exception as e:
            print('Error:', e)
            continue