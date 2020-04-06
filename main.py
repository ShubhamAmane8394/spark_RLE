from spark_context import spark_contexts
from itertools import groupby
import re
import sys


sc = spark_contexts().get_spark()

def rle_encoder(input, output):
    # Read the text file
    input_file = sc.textFile("file://{}".format(input))

    # Iterate through the each of the line and encode the line. Put this encoding logic into EncoderAlgoImplementation file
    read_file = input_file.map(lambda x: x if bool(re.search(r'\d', x)) == True else \
        ''.join([str(len(list(v))) + k for k, v in groupby(x)]))

    # check for the diff in sizes of original data and latest encoded data


    # If both are same discard it, else write the encoded data to to output folder
    read_file.coalesce(1).saveAsTextFile("file:///{}".format(output))
    return read_file


if __name__ == '__main__':
    input = sys.argv[0]
    output = sys.argv[1]

    rle_encoder(input, output)
    sc.stop()