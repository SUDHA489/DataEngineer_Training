sq=[i**2 for i in range(1,6)]
ev=[x for x in range(10) if x%2==0]
print(sq)
print(ev)


sqMap=list(map(lambda i:i**2,[1,2,3,4,5]))
print(sqMap)
evfilter=list(filter(lambda i:i%2==0,[1,2,3,4,5,6]))
print(evfilter)