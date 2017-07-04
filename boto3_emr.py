import boto3    
import time

def create_emr():
	client = boto3.client('emr', region_name='cn-north-1')
	response = client.run_job_flow(
    		Name="My Spark Cluster",
    		ReleaseLabel='emr-5.5.0',
    		Instances={
        		'MasterInstanceType': 'm4.xlarge',
        		'SlaveInstanceType': 'm4.xlarge',
        		'InstanceCount': 4,
        		'TerminationProtected': False,
        		'KeepJobFlowAliveWhenNoSteps': False,
			'Ec2SubnetId':'subnet-0b53ab7c',
			'Ec2KeyName':'dongaws-2nd-team'
    			},
    		Applications=[
        		{		
            		'Name': 'Spark'
        		}
    		],
   		 Steps=[
    			{
        			'Name': 'Setup Debugging',
        			'ActionOnFailure': 'TERMINATE_CLUSTER',
        			'HadoopJarStep': {
            				'Jar': 'command-runner.jar',
            				'Args': ['state-pusher-script']
        			}
    			},
    			{
        			'Name': 'Run Spark',
        			'ActionOnFailure': 'TERMINATE_JOB_FLOW',
        			'HadoopJarStep': {
            			'Jar': 'command-runner.jar',
            			'Args': ['spark-submit','--class','org.apache.spark.examples.SparkPi','/usr/lib/spark/examples/jars/spark-examples.jar','10']
        		}
    			}
    			],
    		VisibleToAllUsers=True,
    		JobFlowRole='EMR_EC2_DefaultRole',
    		ServiceRole='EMR_DefaultRole',
    		ScaleDownBehavior='TERMINATE_AT_INSTANCE_HOUR'
		)
	return response['JobFlowId']

def query_step_status(jobId,stepNum):
	client = boto3.client('emr', region_name='cn-north-1')
	query=client.list_steps(ClusterId=jobId)
	state=query["Steps"][stepNum]['Status']['State']
	if state=='FAILED':
		return 'Status:'+query["Steps"][stepNum]['Status']['State']+':Reason:'+query["Steps"][stepNum]['Status']['FailureDetails']['Reason']+':Message:'+query["Steps"][stepNum]['Status']['FailureDetails']['Message']
	else:
		return 'Status:'+query["Steps"][stepNum]['Status']['State']

