from pyspark import SparkContext 
from operator import add
import sys


def func1(x):
	l=x[1]
	v=[0]*100
	for i in l:
		v[int(i)]=1
	return (x[0],v)

def func2(t):
	l=t[1]
	sig={}
	for x in range(0,100):
		if(l[x]==1):
			#print "x=",x
			for i in range(0,20):
				h=((3*x) + (13*i))%100
				if(h<sig.get(i,sys.maxint)):
					sig[i]=h
	
	return (t[0],sig.items())
def func3(x):
	l=x[1]
	b1=l[0:4]
	b2=l[4:8]
	b3=l[8:12]
	b4=l[12:16]
	b5=l[16:20]
	temp=[]
	temp.append((1,[(x[0],b1)]))
	temp.append((2,[(x[0],b2)]))
	temp.append((3,[(x[0],b3)]))
	temp.append((4,[(x[0],b4)]))
	temp.append((5,[(x[0],b5)]))
	return temp


def func4(x):
	l=x[1]
	u={}
	cand=[]
	for i in l:
		u[i[0]]=i[1]
	su=u.keys()
	#su.sort(key=lambda x: int(x.split('U')[1]))
	#print ">>",su
	for j in su:
		for k in su:
			if((cmp(u[j],u[k])==0) and (j!=k)):
				cand.append((j,k))
	return cand

def func5(x):
	user={}
	
	f={}
	for i in ds:
		user[i[0]]=i[1]
	for i in x:
		j=getjac(user,i[0],i[1])
		l=f.get(i[0],[])
		l.append((i[1],j))
		#l.sort(comp)
		f[i[0]]=l
	
	return f.items()

def getjac(user,one,two):
	l1=user[one]
	l2=user[two]
	total=len(set(l1+l2))
	
	cnt=0
	for i in l1:
		for k in l2:
			if(i==k):
				cnt+=1
	#print cnt/total
	return float(cnt)/total

def func6(x):
	first=x[0].replace("U","")
	second=x[1]
	l=[]
	
	#s=x[0]+":"
	for i in x[1]:
		l.append((i[0],i[1]))
	return (int(first),(x[0],l))

def func7(x,y):
	p1=x[1]
	p2=y[1]

	return (x[0],list(set(p1+p2)))


def func9(x):
	s1=x[1]
	
	s=s1[1]
	
	s.sort(comp)
	#print "s="+s1[0]+":",s
	if len(s)>5:
		s=s[0:5]
	
	s.sort(key=lambda x: int(x[0].split("U")[1]))
	#print "After sort:",s
	f=s1[0]+":"
	for i in s:
		
		f=f+i[0]+","
	
	return (x[0],f[:-1])

def comp(x,y):
	t1=x
	t2=y
	j1=(t1[1])
	j2=(t2[1])
	uid1=int(t1[0].split("U")[1])
	uid2=int(t2[0].split("U")[1])
	if j1>j2:
		return -1
	elif j2>j1:
		return 1
	elif j2==j1:
		if uid1>uid2:
			return 1
		elif uid2>uid1:
			return -1

sc = SparkContext(appName="inf553")

file_name = sys.argv[1]
out_file=sys.argv[2]

rdd1=sc.textFile(file_name)
rdd1=rdd1.flatMap(lambda x: x.split('\n'))
rdd1=rdd1.map(lambda x: x.split(","))
rdd1=rdd1.map(lambda x: (x[0],list(x[1:])))
ds=rdd1.collect()
#print "Intial dataset:",ds
rdd1=rdd1.map(func1)
rdd1=rdd1.map(func2)
rdd1=rdd1.flatMap(func3)
rdd1=rdd1.reduceByKey(lambda x,y: x+y)
rdd1=rdd1.map(func4)
rdd1=rdd1.flatMap(func5)
rdd1=rdd1.map(func6)
#rdd1=rdd1.distinct()
rdd1=rdd1.reduceByKey(func7)
#rdd1=rdd1.map(func8)
rdd1=rdd1.map(func9)
rdd1=rdd1.sortByKey(True)
rdd1=rdd1.map(lambda x: x[1])
final=rdd1.collect()
#print final
fo = open(out_file, "w")
for v in final:
	v=str(v)
	fo.write(v)
	fo.write("\n")
fo.close()