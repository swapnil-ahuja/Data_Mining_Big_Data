from pyspark import SparkContext 
from operator import add
import sys
import itertools

def phase1(iterator):
	par = list(iterator)
	d={}
	nob=0
	for i in par:
		s=i.split(",")
		nob=nob+1
		for j in s:
			d[int(j)]=d.get(int(j),0)+1
	l=[]
	nob_thresh=nob*float(threshold)
	l_final=[]
	for i in d.items():
		if i[1]>=nob_thresh:
			l.append(i[0])
			l_final.append((i[0],1))
	
	
	di={}
	cnt=2
	final_list=[]
	tl=[]
	tl.append('1')
	
	while len(tl)!=0:
		l2=[] 
		tl=[]
		di.clear()
		for i in par:
			
			s=i.split(",")
			t=[]
			for j in s:
				t.append(int(j))
			temp=list(itertools.combinations(t, cnt))
			l2=list(itertools.combinations(l, cnt))
			
			for y in temp:
				if y in l2:
					di[y]=di.get(y,0)+1
		
		for k in di.items():
			if k[1]>=nob_thresh:
				tl.append((k[0],1))
		#print "tl==",tl
		final_list=final_list+tl
		cnt=cnt+1
		lf=set(l_final+final_list)
	yield(list(lf))

def phase2(iterator):
	par=list(iterator)
	d={}
	for i in par:
		s=i.split(",")
		t=[]
		for j in s:
			t.append(int(j))
		
		
		set1=set(rdd1)
		for k in set1:
			
			if type(k[0]) is tuple:
				ln=len(k[0])
				temp=list(itertools.combinations(t, ln))
			else:
				ln=1
				temp=t
			if k[0] in temp:
				d[k[0]]=d.get(k[0],0)+1
			
	return d.items()

def printf(part): 
	print list(part)
def filter_out(x):
	if x[1]>=nob2:
		return True
	else:
		return False

sc = SparkContext(appName="inf553")

file_name = sys.argv[1]
threshold = sys.argv[2]
out_file=sys.argv[3]

rdd1=sc.textFile(file_name)
print "rdd1==",rdd1.getNumPartitions()
rdd=rdd1.flatMap(lambda x: x.split('\n'))

nob2=rdd1.count()
#print "nob2==",nob2
nob2=nob2*float(threshold)

rdd1=rdd.mapPartitions(phase1)
rdd1=rdd1.reduce(lambda x,y: x+y)
rdd2=rdd.mapPartitions(phase2)

rdd2=rdd2.reduceByKey(add)

rdd2=rdd2.filter(filter_out)
rdd2=rdd2.map(lambda x: x[0])

res=rdd2.collect()
fo = open(out_file, "w")
for v in res:
	v=str(v)
	v=v.replace("(","")
	v=v.replace(")","")
	v=v.replace(" ","")	
	fo.write(v)
	fo.write("\n")
fo.close()


