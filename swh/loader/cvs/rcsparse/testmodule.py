import swh.loader.cvs.rcsparse

f=rcsparse.rcsfile('test,v')
print f.head
print f.branch
s=f.symbols
print s['RELENG_4']
print s.items()
r=f.revs
i=r.items()
print i
print f.getlog(f.sym2rev('RELENG_4')).decode('ascii')
print '1.1' in r
