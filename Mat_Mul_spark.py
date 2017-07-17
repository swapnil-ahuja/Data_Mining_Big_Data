from pyspark import SparkContext 
from operator import add
import sys

def remove(x):
	if(x=='\n' or x=='\r'):
		return False
	else:
		return True

def mul(x):
	a={}
	b={}
	l=[]
	for i in list(x[1]):
		t=i[1]
		if(i[0]=="A"):
			a[t[0]]= int (t[1])
		elif(i[0]=="B"):
			b[t[0]]=int (t[1])
	for i in a.keys():
		for j in b.keys():
			val=a.get(i,0)*b.get(j,0)
			k=(i,j)
			l.append((k, val))
	
	return l

		


sc = SparkContext(appName="inf553")

file_name = sys.argv[1]
file_name2 = sys.argv[2]
out_file=sys.argv[3]
#print file_name
#print file_name2
#print out_file

rdd1=sc.textFile(file_name)
rdd2=sc.textFile(file_name2)
#print rdd1.collect()
rdd1=rdd1.flatMap(lambda x: x.split('\n'))
rdd1=rdd1.map(lambda x: x.split(",")).map(lambda x: (x[1],("A",(x[0],x[2]))))
rdd2=rdd2.flatMap(lambda x: x.split('\n'))
rdd2=rdd2.map(lambda x: x.split(",")).map(lambda x: (x[0],("B",(x[1],x[2]))))
r=rdd1.union(rdd2);
#print r.collect()
r=r.groupByKey()

r=r.map(mul)
#print "After mul"
#print r.collect()
r=r.flatMap(lambda x: x)
output=r.reduceByKey(add).collect()
#print output
fo = open(out_file, "w")
for v in output:
	t=v[0]
	fo.write("%s,%s\t%s" % (t[0],t[1],v[1]))
	fo.write("\n")
fo.close()

