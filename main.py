import boto3_emr as emr
import threading

import time
import Queue

q = Queue.Queue()


class MyThread(threading.Thread):
    def __init__(self, q):
        super(MyThread, self).__init__()
        self.q = q

    def run(self):

	#create emr cluster
	jobID=emr.create_emr()
	
	#query the step status
	while True:
		#query step status
		rs=emr.query_step_status(jobID,0)
		state=rs.split(':')[1]

		#put result into the queue
        	self.q.put(rs)

        	if state=='COMPLETED' or state=='FAILED' or state=='CANCELLED' or state=='INTERRUPTED':
			print "Status:"+state+", son thread break"
			break


state=''
threads = []
threads.append(MyThread(q))
for mt in threads:
    mt.start()

while True:
	if not q.empty():
		message=q.get()
		state=message.split(':')[1]
		print message 
    		if state=='COMPLETED' or state=='FAILED' or state=='CANCELLED' or state=='INTERRUPTED':
			print "Status"+state+", father thread break"
			break
