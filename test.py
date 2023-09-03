d={}
for l in open('data_test','r'):
   v=l.split(" ")
   b,t,i,o=int(v[0]),int(v[1]),int(v[2]),int(v[3])
   for x in range(4+i,4+i+o):
    vv=int(float(v[x])*100000000)
    if vv>0: d[v[x+o]]=(b,vv)
   for x in range(4,4+i): d.pop(v[x], None)
print(d) #creates 5.4MB text output of dictionary d based on 80k lines in file data_test
