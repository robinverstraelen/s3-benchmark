import boto3
from botocore.config import Config
import s3transfer
import time
import numpy
import os
import random
from decimal import Decimal
import json

# setup
my_config = Config(
    connect_timeout=0.2,
    read_timeout=0.2,
)
client = boto3.client('s3', 'eu-west-1', config=my_config)
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('s3-benchmark')
instanceid = os.popen(
    "curl http://169.254.169.254/latest/meta-data/instance-id").read()
print(f"instance id = {instanceid}")

# upload
uploadTimes = []


def uploadFile(counter):
    start = time.perf_counter()
    #transfer.upload_file("/home/ec2-user/environment/output.dat", 'av-benchmark-s3', str(counter) + '/testfile')
    client.upload_file('/home/ec2-user/environment/output.dat',
                       'av-benchmark-s3', 'testfile' + str(counter))
    end = time.perf_counter() - start
    uploadTimes.append(end)
    print(f"Upload time: {end}s")


# download
downloadTimes = []


def downloadFile():
    # try:
    partition = random.randint(0, 99)
    start = time.perf_counter()
    #transfer.download_file('av-benchmark-s3', str(partition) + '/testfile', "/home/ec2-user/environment/output.dat")
    client.download_file('av-benchmark-s3', 'testfile' +
                         str(partition), "/home/ec2-user/environment/output.dat")
    end = time.perf_counter() - start
    downloadTimes.append(end)
    print(f"Download time: {end}s")
    item = {
        'node': str(instanceid),
        'timestamp': str(int(time.time())),
        'latency': end
    }

    table.put_item(
        Item=json.loads(json.dumps(item), parse_float=Decimal)
    )
    # except:
    #    print("Download failed")

# for x in range(0,100):
#    os.system('dd if=/dev/zero of=output.dat  bs=50M  count=1')
#    uploadFile(x)
#    os.system('rm output.dat')


while 1 == 1:
    downloadFile()
    time.sleep(random.uniform(0.0, 4.0) * 60)


print(f"p90: {numpy.percentile(numpy.array(downloadTimes), 90)}")
print(f"p95: {numpy.percentile(numpy.array(downloadTimes), 95)}")
print(f"p99: {numpy.percentile(numpy.array(downloadTimes), 99)}")
print(f"max: {numpy.max(numpy.array(downloadTimes))}")
