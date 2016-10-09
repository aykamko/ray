import ray

num_workers=2
ray.init(start_ray_local=True, num_workers=num_workers)

@ray.remote
def return_foo(rkv_objid):
  rkv = ray.KeyValueStore(dict_objid=rkv_objid)
  return rkv['foo']

rkv = ray.KeyValueStore()

rkv['foo'] = 'hello world'
results = []
for _ in xrange(num_workers):
  results.append(return_foo.remote(rkv.dict_objid))
print ray.get(results)

rkv['foo'] = 'one two three four'
results = []
for _ in xrange(num_workers):
  results.append(return_foo.remote(rkv.dict_objid))
print ray.get(results)
