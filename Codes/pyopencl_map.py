import pyopencl as cl
import pyopencl.clrandom as clrand
from pyopencl.elementwise import ElementwiseKernel
import numpy as np

ctx = cl.create_some_context()
queue = cl.CommandQueue(ctx)

n = 10 ** 6 
a = clrand.rand(queue, n, np.float32) 
b = clrand.rand(queue, n, np.float32)

c1 = 5 * a + 6 * b
result_np = c1.get()

lin_comb = ElementwiseKernel(ctx,
    "float a, float *x, float b, float *y, float *c",
    "c[i] = a * x[i] + b * y[i]")
c2 = cl.array.empty_like(a)
lin_comb(5, a, 6, b, c2)
result_np = c2.get()