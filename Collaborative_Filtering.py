import numpy as np
import sys
import math
def sum_vsj2(v,s,m,r):
	sum=0.0
	for j in range(0,m):
		if not np.isnan(m_arr[r][j]):
			sum=sum+(v[s][j]*v[s][j])
	#print"sumvsj=",sum
	return sum

def sum_uir2(u,r,n,s):
	sum=0.0
	for i in range(0,n):
		if not np.isnan(m_arr[i][s]):
			sum=sum+(u[i][r]*u[i][r])
	
	return sum


def sum_uvx(u,v,r,f,s,j):
	sum=0.0
	for k in range(0,f):
		if k!=s:
			sum=sum+(u[r][k]*v[k][j])
	#print"sumx=",sum
	return sum

def sum_uvy(u,v,r,i,f,s):
	sum=0.0
	for k in range(0,f):
		if k!=r:
			sum=sum+(u[i][k]*v[k][s])
	return sum

def findx(u,v,m,f,r,s):
	den=sum_vsj2(v,s,m,r)
	#print "den=",den
	num=0.0
	for j in range(0,m):
		if not np.isnan(m_arr[r][j]):
			temp=v[s][j]*(m_arr[r][j]-sum_uvx(u,v,r,f,s,j))
			num=num+temp	
	#print "num=",num
	return num/float(den)

def findy(u,v,n,f,r,s):
	den=sum_uir2(u,r,n,s)
	num=0.0
	for i in range(0,n):
		if not np.isnan(m_arr[i][s]):
			temp=u[i][r]*(m_arr[i][s]-sum_uvy(u,v,r,i,f,s))
			num=num+temp
	

	return num/float(den)
def cnt_nonblank(z):
	cnt=0
	for i in z:
		for k in i:
			if not np.isnan(k):
				cnt+=1
	return cnt

file_name = sys.argv[1]
n=int(sys.argv[2])
m=int(sys.argv[3])
f=int(sys.argv[4])
k=int(sys.argv[5])
#tprint n,m,f,k

m_arr=np.zeros((n,m))

m_arr.fill(np.nan)
lines=open(file_name)
for line in lines:
	t=line.replace("\n","").split(",")
	m_arr[int(t[0])-1][int(t[1])-1]=float(t[2])
#print m_arr

cnt=cnt_nonblank(m_arr)
#print cnt

u=np.ones((n,f))
v=np.ones((f,m))
#print u
#print v
final=[]
for g in range(0,k):
	for r in range(0,n):
		for s in range(0,f): 
			u[r][s]=findx(u,v,m,f,r,s)
	
	
	#print u
	for s in range(0,m):
		for r in range(0,f):
			v[r][s]=findy(u,v,n,f,r,s)
	#print v
	#u[2][0]=findx(u,v,m,f,2,0)
	
	mt=np.dot(u, v)
	zl=np.isnan(m_arr)
	mt[zl]=np.nan
	#print "mt=",mt
	
	diff= np.subtract(m_arr,mt)
	#print diff
	
	sq=np.multiply(diff,diff)
	#print sq
	t1=np.isnan(sq)
	sq[t1]=0
	#print sq
	
	sqe=np.sum(sq)
	#print sqe
	rmse= math.sqrt(sqe/float(cnt))
	print "%.4f" % rmse
	
