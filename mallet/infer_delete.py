import pyspark
from pyspark import SparkConf
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

def deletefunction(k, prfolder):
	import os, shutil
	folder = prfolder
	for the_file in os.listdir(folder):
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


#    logger.info('received arguments:{}'.format(args))


    conf = SparkConf().setAppName("Predicition with mallet")
    conf.set('spark.executor.cores', '5')
    sc = pyspark.SparkContext(conf=conf)
    s3_client = get_s3client()

    keys = fetch_s3_keys('cypher-emr-input', s3_client)

    # filter out only png images.
    # you can also choose to check the content-type headers by doing
    # a head call against each S3-Key

    keys = filter(lambda x: x.endswith('.txt'), keys)

    # number of keys

    n_partitions = 60 // 10

#    logger.info('number of keys from s3: {}'.format(n_keys))


#    logger.debug('Keys:{}'.format(keys))

    n_partitions = len(keys) // 10
#    logger.info("number of keys:{}, n_partitions:{}".format(len(keys), n_partitions))

    # we will create partitions of args['batch']
    rdd = sc.parallelize(keys, numSlices=n_partitions)
#    logger.info('created rdd with {} partitions'.format(rdd.getNumPartitions()))
    bucket = 'cypher-emr-out-new'
    sc.broadcast(bucket)

    rdd = rdd.mapPartitions(lambda k : deletefunction(k, '/mnt/tmp/mallet/mallet-input/'))
    rdd = rdd.mapPartitions(lambda k : deletefunction(k, '/mnt/tmp/mallet/mallet-output/'))
    rdd = rdd.mapPartitions(lambda k : deletefunction(k, '/mnt/tmp/mallet/feature_Archive/'))
    output = rdd.collect()

if __name__ == '__main__':
    main()