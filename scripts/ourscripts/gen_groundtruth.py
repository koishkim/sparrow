import sys
import numpy

length = len(sys.argv) # total number of arguments
if length != 4:
    print "Wrong number of arguments."
    print "Need: [number of nodes] [minimum score] [maximum score]"
    sys.exit(1)

try:
    n = int(sys.argv[1]) # total number of nodes
except ValueError:
    print "1st argument: not an integer."
    sys.exit(1)

try:
    minW = float(sys.argv[2]) # minimum score
except ValueError:
    print "2nd argument: not a float."
    sys.exit(1)

try:
    maxW = float(sys.argv[3]) # maximum score
except ValueError:
    print "3rd argument: not a float."
    sys.exit(1)

# ground truth score vector generation
f = open('groundtruth.txt', 'w')
for i in range(n):
    f.write(str(numpy.random.uniform(minW, maxW)))
    f.write("\n")

f.close()

sys.exit(0)
