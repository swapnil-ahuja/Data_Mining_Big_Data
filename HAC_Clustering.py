import numpy as np
import sys
from scipy.sparse import csr_matrix
import math
import heapq as hq
import time

def cos(A,B):
    C=A.multiply(B)
    C=C.sum()
    #print C
    A1=A.multiply(A)
    B1=B.multiply(B)
    #print A
    #print B
    D1=math.sqrt(A1.sum())
    D2=math.sqrt(B1.sum())
    D=D1*D2
    #print D

    return C*1.0/D

def get_cent(l):
	x=l
	#print "x=",x
	n=len(x)
	#print "n=",n
	return csr_matrix(uv[x,:].sum(axis=0)/n)
	
def check_valid(a,l):
	for i in l:
		if i in a:
			return True 		
	return False
	
def gen_intial_clus(N,uv):
	c={}
	for i in range(1,N):
		c[str(i)]=([i],uv.getrow(i))
	return c

file_name=sys.argv[1]
file1=open(file_name,"r")
no_K=int(sys.argv[2])
f1=file1.read().splitlines()
N=int(f1[0])+1
#uv=csr_matrix((int(f1[0])+1, int(f1[1])), dtype=np.float64)
#uv=np.zeros((N,int(f1[1])))
docid=[]
wordid=[]
val=[]
for l in f1[3:]:
    x=l.split(" ")
    #print x
    #uv[int(x[0])][int(x[1])-1]=int(x[2])
    docid.append(int(x[0]))
    wordid.append(int(x[1])-1)
    val.append(float(x[2]))
uv=csr_matrix((val, (docid, wordid)), shape=(N, f1[1]))
total_df=uv.nonzero()[1]
#print uv
#print (total_df)
#print int(f1[1])
for i in range(0,int(f1[1])):
    #print i
    #df=len(uv[uv[:,i]>0,i])
    df=len(total_df[total_df==i])
    
    t=(N)*1.0/(df+1)
    #print "N=",N
    #print "df=",df
    #print math.log(t,2)
    uv[:,i]=uv.getcol(i).multiply(math.log(t,2))
    
    

    
#print ("uv= tf*idf",uv)
for i in range(1,N):
    x=uv.getrow(i).multiply(uv.getrow(i))
    x=x.sum()
    
    x=math.sqrt(x)
    #print("x=",x)
    uv[i,:]=uv.getrow(i)/x
#print uv






#print (uv[:,117])
h=[]
for i in range(1,N):
	#print "i",i
	for j in range(1,N):
		

		if i!=j:
			hq.heappush(h,(1-cos(uv.getrow(i),uv.getrow(j)),[[i],[j]]))


oc=[]
clus=gen_intial_clus(N,uv)




#for k in range(0,10):
while(len(clus)>no_K):
	
	p1=hq.heappop(h)
	p1[1].sort()
	#print "p1=",p1
	ele=sum(p1[1],[])
	#print "ele=",ele
	#print oc
	if(check_valid(p1[1],oc)):
		#print "old_val"
		continue
		
		
		
	
	nc1=get_cent(ele)
	#print "nc1",nc1
	#insert into old list and del from clus
	for p in p1[1]:
		oc.append(p)
		del clus[','.join(map(str, p))]

	
	for i in clus.values():
			
			hq.heappush(h,(1-cos(nc1,i[1]),[i[0],ele]))

	clus[','.join(map(str, ele))]=(ele,nc1)
	#print len(clus)

#print "FINAL+++"
for i in clus.keys():
	s=i.split(",")
	s.sort()
	
	#print len(s)
	print ','.join(map(str, s))


