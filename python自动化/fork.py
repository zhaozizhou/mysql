 # coding=utf-8
import os
import time

p = os.fork()

if p == 0:
    time.sleep(10)
    print('执行子进程, pid={} ppid={} p={}'.format(os.getpid(), os.getppid(), p))
else:
    time.sleep(5)
    print('执行主进程, pid={} ppid={} p={}'.format(os.getpid(), os.getppid(), p))