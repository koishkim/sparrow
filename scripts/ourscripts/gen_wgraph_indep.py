import sys
import numpy

length = len(sys.argv) # total number of arguments
if length != 4:
    print "Wrong number of arguments."
    print "Need: [filename of ground truth] [number of samples] [number of comparisons per sample]"
    sys.exit(1)

try:
    f_in = open(sys.argv[1], 'r') # file containing ground truth vector
except IOError:
    print "1st argument: no such file."
    sys.exit(1)

try:
    m = int(sys.argv[2]) # total number of samples
except ValueError:
    print "2nd argument: not an integer."
    sys.exit(1)

try:
    k = int(sys.argv[3]) # total number of comparisons per sample
except ValueError:
    print "3rd argument: not an integer."
    sys.exit(1)

w = f_in.readlines() # ground truth score vector
n = len(w) # total number of items
for x in range(n):
    w[x] = float(w[x]) # conversion from string to float

# sample graph generation
#f_log = open('gensamples_log.txt', 'w')

samples = []
numsamples = 0
while True:
    i = numpy.random.random_integers(n)
    while True:
        j = numpy.random.random_integers(n)
        if j != i:
            break

    if any("%d %d " % (i, j) in s for s in samples):
#	f_log.write("old: (%d %d) is already connected.\n" % (i, j))
        continue
    else:
#        f_log.write("new: (%d %d) is newly connected.\n" % (i, j))
        p_ij = w[j-1] / (w[i-1] + w[j-1])
        a_ij = numpy.random.binomial(k, p_ij)
        wgt_ij = float(a_ij) / float(k)
        samples.append("%d %d %f\n" % (i, j, wgt_ij))
        samples.append("%d %d %f\n" % (j, i, 1-wgt_ij))
        numsamples += 1

    if numsamples == m:
        break

f_out = open('sample_wgraph_indep.tsv', 'w')

for x in range(2*m):
    f_out.write(samples[x])

f_in.close()
f_out.close()
#f_log.close()

sys.exit(0)
