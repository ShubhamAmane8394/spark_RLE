from spark_context import spark_contexts
from itertools import groupby
import re
import sys
from pyspark.serializers import PickleSerializer, AutoBatchedSerializer



sparkcontext = spark_contexts().get_spark_context()

def _to_java_object_rdd(rdd):
    """ Return a JavaRDD of Object by unpickling
    It will convert each Python object into Java object by Pyrolite, whenever the
    RDD is serialized in batch or not.
    """
    rdd = rdd._reserialize(AutoBatchedSerializer(PickleSerializer()))
    return rdd.ctx._jvm.org.apache.spark.mllib.api.python.SerDe.pythonToJava(rdd._jrdd, True)

def get_size(file):
    JavaObj = _to_java_object_rdd(file)
    nbytes = sparkcontext._jvm.org.apache.spark.util.SizeEstimator.estimate(JavaObj)
    return nbytes

def rle_encoder(input, output):

    # Read the text file
    input_file = sparkcontext.textFile("file://{}".format(input))
    input_file_size = get_size(input_file)
    print(input_file_size)
    # Iterate through the each of the line and encode the line. Put this encoding logic into EncoderAlgoImplementation file
    read_file = input_file.map(lambda x: x if bool(re.search(r'\d', x)) == True else \
        ''.join([str(len(list(v))) + k for k, v in groupby(x)]))
    output_file_size  = get_size(read_file)
    print(output_file_size)
    # check for the diff in sizes of original data and latest encoded data

    if int(input_file_size) >= int(output_file_size):
        print(True,"\n\n\n")
        read_file.coalesce(1).saveAsTextFile("file:///{}".format(output))
    else:
        print(False,"\n\n\n")
        # read_file.coalesce(1).saveAsTextFile("file:///{}".format(output))

    # If both are same discard it, else write the encoded data to to output folder

    return read_file


if __name__ == '__main__':
    input = sys.argv[1]
    output = sys.argv[2]
    print(input,"\n",output)
    rle_encoder(input, output)
    sparkcontext.stop()