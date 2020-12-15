import pyopencl as cl
import pyopencl.clrandom as clrand
import numpy as np

ctx = cl.create_some_context()
queue = cl.CommandQueue(ctx)

n = 10 ** 6  
a = clrand.rand(queue, n, np.float32) 
b = clrand.rand(queue, n, np.float32)


print(a)
print(b)