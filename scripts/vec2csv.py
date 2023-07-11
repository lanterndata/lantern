import sys
import numpy as np
import csv

def fvecs_to_csv(input_file, output_file):

    elem_type = None
    filesuffix = input_file.split(".")[-1]
    if filesuffix == "fvecs":
        elem_type = "float32"
    elif filesuffix == "ivecs":
        elem_type = "int32"
    elif filesuffix == "bvecs":
        raise Exception("fix me")
    else:
        raise Exception("unknown file format %s in %s" % (filesuffix, input_file))


    with open(input_file, 'rb') as f:

        a = np.fromfile(f, dtype='int32')
        d = a[0]
        vectors = a.reshape(-1, d + 1)[:, 1:].copy().view(elem_type)

    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        for vector in vectors:
            writer.writerow([str(vector.tolist())])

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: python vec2csv.py input_file output_file')
    else:
        input_file = sys.argv[1]
        output_file = sys.argv[2]
        fvecs_to_csv(input_file, output_file)
