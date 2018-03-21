import pyspark
from pyspark import SparkConf
import logging
import time
log_fmt = '%(asctime)s -  %(name)s - %(levelname)s %(process)d %(funcName)s:%(lineno)d %(message)s'
logging.basicConfig(format=log_fmt, level=logging.INFO)

logger = logging.getLogger(__name__)
logger.debug ('All modules imported')

def get_all_object_keys(bucket, start_after = '', keys = [], Prefix = ''):
    import boto3
    s3client = boto3.client('s3')
    response = s3client.list_objects_v2(
        Bucket     = bucket,
        # Prefix     = prefix,
        StartAfter = start_after, 
        Prefix = Prefix )

    if 'Contents' not in response:
        return keys

    key_list = response['Contents']
    last_key = key_list[-1]['Key']

    keys.extend(key_list)

    return get_all_object_keys(bucket, last_key, keys, Prefix)



def PutMetadata(InputBucket, OutputBucket, InstanceCount, StartTime, EndTime):
    import boto3

    dynamo_client = boto3.client('dynamodb')
    No_of_Inputs = (len(get_all_object_keys(InputBucket)))
    keys_new = []
    keys_out = get_all_object_keys(OutputBucket, keys = keys_new, Prefix = 's3n://' + InputBucket )
    print keys_out
    No_of_Outputs = (len(keys_out))
    processed = No_of_Inputs - No_of_Outputs
    if processed >=0:
        Failed = str(processed)
    else:
        Failed = '0'
    Success = str(No_of_Outputs)
    TotalTime = EndTime - StartTime 
    response = dynamo_client.put_item(TableName='Cypher-Meta-Ner', Item={'InputBucket':{'S':InputBucket}, 'NInputs':{'S':str(No_of_Inputs)}, 'NOutputs ':{'S':str(No_of_Outputs)}, 'Success':{'S':Success}, 'Failed':{'S':Failed}, 'StartTime':{'S':str(StartTime)}, 'EndTime':{'S':str(EndTime)}, 'InstanceCount':{'S':str(InstanceCount)}, 'TotalTime':{'S':str(TotalTime)}})
    return response




if __name__ == '__main__':

    from pyspark import SparkConf
    import logging
    import pickle
    import codecs
    import argparse
    _start = time.time()
    parser = argparse.ArgumentParser(description='Take bucket inputs')
    parser.add_argument('--inputbucket', '-i', required=True, type=str, help='Name for input S3 Bucket')
    parser.add_argument('--outputbucket', '-o', required=True, type=str, help='Name for output S3 BUcket')
    parser.add_argument('--modelbucket', '-m', required=True, type=str, help='Name for Model S3 BUcket')
    parser.add_argument('--instancecount', '-n', required=True, type=int, help='Total number of Instances launched in cluster')
#    parser.add_argument('--ftype', '-f', required=False, type=float, help='Value of the metric')
    args = parser.parse_args()
    InputBucket = args.inputbucket
    OutputBucket = args.outputbucket
    ModelBucket = args.modelbucket
    InstanceCount = args.instancecount
    FileType = "xml"
    InputLocation = "s3n://" + InputBucket + "/*." + FileType
    conf = SparkConf().setAppName("Spacy Prediction")
    conf.set('spark.executor.instances', '2')
    conf.set('spark.executor.cores', '5')
    conf.set('spark.executor.memory', '18g')
    sc = pyspark.SparkContext(conf=conf)
    sc.addPyFile('s3://cypher-models/predict_code.py')
    from predict_code import xml2txt
    from predict_code import assert_func
    from predict_code import s3Upload
    import pyspark
#    input_bucket = "s3n://" + args.input-bucket + "/*.xml"
    rdd = sc.wholeTextFiles(InputLocation, minPartitions=8000)
    rdd = rdd.mapValues(lambda xml_f: xml2txt(ModelBucket, xml_f))    
    rdd= rdd.mapValues(lambda data: assert_func(ModelBucket, data))
    rdd = rdd.mapPartitions(lambda predicted_data: s3Upload(OutputBucket, predicted_data))
    output = rdd.collect()
    endtime = time.time()
    metadata = PutMetadata(InputBucket, OutputBucket, InstanceCount, _start, endtime)
    print metadata
#   print type(output)
#    print output