import sys
import numpy
import math

length = len(sys.argv) # total number of arguments
if length != 3 and length != 4:
    print "Wrong number of arguments."
    print "Need: [filename of ground truth] [number of comparisons per edge] or"
    print "Need: [filename of ground truth] [edge probability] [number of comparisons per edge]"
    sys.exit(1)

try:
    f_in = open(sys.argv[1], 'r') # file containing ground truth vector
except IOError:
    print "1st argument: no such file."
    sys.exit(1)

w = f_in.readlines() # ground truth score vector
n = len(w) # total number of items
for x in range(n):
    w[x] = float(w[x]) # conversion from string to float

if length == 4: # p is specified
    try:
        p = float(sys.argv[2]) # edge linking probability
    except ValueError:
        print "2nd argument: not a float."
        sys.exit(1)

    try:
        k = int(sys.argv[3]) # total number of comparisons per edge
    except ValueError:
        print "3rd argument: not an integer."
        sys.exit(1)

if length == 3: # p is set to 'c * log(n) / n', where c is a constant
    try:
        k = int(sys.argv[2]) # total number of comparisons per edge
    except ValueError:
        print "2nd argument: not an integer."
        sys.exit(1)
    
    c = 1 # now it is a magic number
    p = c * math.log(n) / n

# erdos-renyi graph generation
f_out = open('sample_wgraph_erdosrenyi.tsv', 'w')

for i in range(n):
    for j in range(i+1, n):
        isconn = numpy.random.binomial(1, p)

        if isconn:
            p_ij = w[i] / (w[i] + w[j])
            a_ij = numpy.random.binomial(k, p_ij)
            wgt_ij = float(a_ij) / float(k)
            f_out.write("%d %d %f\n" % (i+1, j+1, wgt_ij))
            f_out.write("%d %d %f\n" % (j+1, i+1, 1-wgt_ij))

f_in.close()
f_out.close()

sys.exit(0)
