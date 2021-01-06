解决redis设备替换问题

1.sentinel name  表
2.老设备列表
3.新设备列表

流程
1.取sentinel列表值找到目前的redis主
2.检查主有几个sentinel
3.检查主有几个从
4.检查从是否在新设备列表
5.检查切换权重参数（未测试）
6.切换
7.再次通过sentinel查看主是否在 3步骤中
8.检查redis连接（使用get命令测试）
9切换成功