import pyspark
from pyspark import SparkConf

def get_args():
    import argparse
    parser = argparse.ArgumentParser(description="Batch Inference using MXNet and Spark")
    parser.add_argument('--bucket', help='S3 bucket holding the input files', required=True)
    
    parser.add_argument('--batch', type=int, default=32, help='Number of images to process at a time', required=True)
    parser.add_argument('--access_key', help='AWS access Key', required=False, default=None)
    parser.add_argument('--secret_key', help='AWS access secret Key', required=False, default=None)
    
    args = vars(parser.parse_args());
    return args

def get_s3client(access_key=None, secret_key=None):
    """
    Returns a boto3 instantiated s3 client

    If you do not pass the access key and secret key, this routine expects that the
    default profile in ~/.aws/config or the EC2 Instance role has the right set of
    credentials to access this bucket.
    
    """
    import  boto3
    if access_key and secret_key:
        return boto3.client('s3', access_key, secret_key)
    else:
        return boto3.client('s3')

def fetch_s3_keys(bucket, s3_client):
    """
    Fetch S3 Keys from the given S3 bucket and prefix, return the list.

    Parameters:
    ----------
    bucket: str, mandatory
        Bucket from which keys have to be fetched.
    prefix: str, optional
        Uses the prefix to fetch keys from the bucket.
    s3_client: boto3.client, mandatory
        boto3 s3 client

    Returns
    -------
    list of s3 keys

    """
    # Note: We import the libraries needed within the function so Spark does
    #      not have serialize the libraries to all the workers,
    #      otherwise it could fail during Serialization/Deserialization
    #      using the pickle methods.
    import boto3
    
    all_keys = []
    more_pages = True
    next_page_token = ''
 
    while more_pages:
        if next_page_token == '':
            objects = s3_client.list_objects_v2(Bucket=bucket)
        else:
            objects = s3_client.list_objects_v2(Bucket=bucket, ContinuationToken=next_page_token)
        if not objects:
            break

        next_page_token = ''

        keys = [object['Key'] for object in objects.get('Contents', [])]
        all_keys.extend(keys)

        more_pages = objects['IsTruncated']
        if 'NextContinuationToken' in objects:
            next_page_token = objects['NextContinuationToken']

    return all_keys

def download_object(bucket, key):
    """
    This routine downloads the s3 key into memory from the s3_bucket
    """
    import boto3
    import os
    import tarfile
    import os
    import sys
    import shutil
#    bucket = 'cypher-emr-input'
#    prefix = 'model310'
    all_keys = key
    parent_folder = '/mnt/tmp/mallet'
    input_path = "/mnt/tmp/mallet/mallet-input"
    s3_client=boto3.client('s3')
    output_path = "/mnt/tmp/mallet/mallet-output"
    feature_path = '/mnt/tmp/mallet/feature_Archive'
#    objects = s3_client.list_objects_v2(Bucket=bucket)
#    keys = [object['Key'] for object in objects.get('Contents', [])]
#    all_keys.extend(keys)
    print all_keys
    if not os.path.exists(parent_folder):
        shutil.rmtree(parent_folder)
        os.makedirs(parent_folder)
        os.makedirs(input_path)
        os.makedirs(output_path)
        os.makedirs(feature_path)
    for s3_key in all_keys:
        if not s3_key.endswith("/"):
            s3_client.download_file(bucket, s3_key, input_path + '/' + s3_key)
    print "Input files downloaded"
    model_bucket = 'cypher-models'
    s3_client.download_file('cypher-models', 'PreProcessing_Test.py', '/mnt/tmp/mallet/' + 'PreProcessing_Test.py')
    s3_client.download_file('cypher-models', 'editing.py', '/mnt/tmp/mallet/' + 'editing.py')
    s3_client.download_file('cypher-models', 'nouncrf', '/mnt/tmp/mallet-2.0.8/' + 'nouncrf')
    fname = '/mnt/tmp/mallet-2.0.8.tar.gz'
    return parent_folder

def s3upload():

    import os
    import sys
    import boto3

# get an access token, local (from) directory, and S3 (to) directory
# from the command-line
    local_directory = '/mnt/tmp/mallet/mallet-output'
    bucket = 'cypher-emr-out-new'
    destination = 'check'

    client = boto3.client('s3')

# enumerate local files recursively
    for root, dirs, files in os.walk(local_directory):
        for filename in files:
            #construct the full local path
            local_path = os.path.join(root, filename)
            #construct the full Dropbox path
            relative_path = os.path.relpath(local_path, local_directory)
            s3_path = os.path.join(destination, relative_path)

    # relative_path = os.path.relpath(os.path.join(root, filename))

            print 'Searching "%s" in "%s"' % (s3_path, bucket)
            try:
                client.head_object(Bucket=bucket, Key=s3_path)
                print "Path found on S3! Skipping %s..." % s3_path

        # try:
            # client.delete_object(Bucket=bucket, Key=s3_path)
        # except:
            # print "Unable to delete %s..." % s3_path
            except:
                print "Uploading %s..." % s3_path
                client.upload_file(local_path, bucket, s3_path)



def predict(bucket, key):
    import os
    import subprocess
    from subprocess import call
    import codecs
    import argparse
    import glob
    import random
    import tarfile
#    print key
    if not os.path.exists('/mnt/tmp/mallet-2.0.8'):
        os.system(" wget -qO- http://mallet.cs.umass.edu/dist/mallet-2.0.8.tar.gz | tar xvz -C /mnt/tmp/")
    parent_folder = download_object(bucket, key)
    
    input_file = parent_folder + '/mallet-input/'
    output_path = parent_folder + '/mallet-output/'
    feature_path = parent_folder + '/feature_Archive/'
    data_folder = glob.glob(input_file + '*.txt')
#    data_folder = [input_file + k for k in key]
    random.shuffle(data_folder)
    mallet_exec = "java -cp "
    input_path = "/mnt/tmp/mallet-2.0.8/class:/mnt/tmp/mallet-2.0.8/lib/mallet-deps.jar"
    model_file = 'nouncrf'  # Model File
    mallet_path = '/mnt/tmp/mallet-2.0.8'
    file_path = "/mnt/tmp/mallet"
#    count = 0
    for f in data_folder:
        #Feature Extraction 
        os.chdir(file_path)
        feature = 'feature_' + os.path.basename(f) + '.txt'
        command = 'python PreProcessing_Test.py -i ' + os.path.basename(f) + ' -o ' + feature
        call(command, shell=True)

    # Data including all features = feature_path + feature
    
    # Mallet prediction (only labels in the output file)
        os.chdir(mallet_path)
        Output_p = feature_path + os.path.basename(f).split('.')[0]
        command = mallet_exec + input_path + " cc.mallet.fst.SimpleTagger" + " --model-file " + model_file + " " + feature_path + feature
        call(command, shell=True, stdout=open(Output_p, 'a'), stderr=subprocess.STDOUT)

    # Final Output in JSON using feature file(only token column needed) and mallet prediction file (only labels needed)
        os.chdir(file_path)
        json_output = os.path.basename(f).split('.')[0] + '.json'
        command = 'python editing.py -f ' + feature + ' -l ' + os.path.basename(f).split('.')[0] \
              + ' -o ' + 'final_' + json_output
        call(command, shell=True)
#        count += 1
    upload = s3upload()
    return key

def deletefunction(k, prfolder):
    import os, shutil
    folder = prfolder
    for the_file in k:
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
             #elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as e:
            print(e)
    return k





def main():
    import pyspark
    from pyspark import SparkConf
    import logging
    import pickle
    import codecs

    args = get_args()
#    logger.info('received arguments:{}'.format(args))


    conf = SparkConf().setAppName("Predicition with mallet")
    conf.set('spark.executor.cores', '5')
    sc = pyspark.SparkContext(conf=conf)
    s3_client = get_s3client(args['access_key'], args['secret_key'])

    keys = fetch_s3_keys(args['bucket'], s3_client)

    # filter out only png images.
    # you can also choose to check the content-type headers by doing
    # a head call against each S3-Key

    keys = filter(lambda x: x.endswith('.txt'), keys)

    # number of keys
    n_keys = len(keys)
    if n_keys < args['batch']:
        args['batch'] = n_keys

    n_partitions = n_keys // args['batch']

#    logger.info('number of keys from s3: {}'.format(n_keys))

    # if keys cannot be divided by args['batch'] .
    if (n_partitions * args['batch'] != n_keys):
        keys.extend(keys[: args['batch'] - (n_keys - n_partitions * args['batch'])])

#    logger.debug('Keys:{}'.format(keys))

    n_partitions = len(keys) // args['batch']
#    logger.info("number of keys:{}, n_partitions:{}".format(len(keys), n_partitions))

    # we will create partitions of args['batch']
    rdd = sc.parallelize(keys, numSlices=n_partitions)
#    logger.info('created rdd with {} partitions'.format(rdd.getNumPartitions()))

    sc.broadcast(args['bucket'])

    rdd = rdd.mapPartitions(lambda k : predict(args['bucket'], k))
    rdd = rdd.mapPartitions(lambda k : deletefunction(k, '/mnt/tmp/mallet/mallet-input/'))
    rdd = rdd.mapPartitions(lambda k : deletefunction(k, '/mnt/tmp/mallet/mallet-output/'))
    rdd = rdd.mapPartitions(lambda k : deletefunction(k, '/mnt/tmp/mallet/feature_Archive/'))
    output = rdd.collect()
    print output



if __name__ == '__main__':
    main()