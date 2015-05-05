import sys
import numpy
import math

length = len(sys.argv) # total number of arguments
if length != 2:
    print "Wrong number of arguments."
    print "Need: [number of nodes]"
    sys.exit(1)

try:
    n = int(sys.argv[1]) # total number of nodes
except ValueError:
    print "1st argument: not an integer."
    sys.exit(1)

# graph generation
f_out = open('sample_nwgraph_erdosrenyi.tsv', 'w')

p = math.log(n) / n

for i in range(n):
    for j in range(i+1, n):
        isconn = numpy.random.binomial(1, p)

        if isconn:
            f_out.write("%d %d\n" % (i+1, j+1))

f_out.close()

sys.exit(0)
