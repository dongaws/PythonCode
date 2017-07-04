import threading  
  
import time  
import Queue
  
q = Queue.Queue()  
  
  
class MyThread(threading.Thread):  
    def __init__(self, q):  
        super(MyThread, self).__init__()  
        self.q = q  
  
    def run(self):  
        self.q.put("Pending")  
  
  
count = 0  
threads = []  
threads.append(MyThread(q))  
for mt in threads:  
    mt.start()  
print "start time: ", time.ctime()  
while True:  
    if not q.empty():  
        print q.get()  
        break  
