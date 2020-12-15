import pyopencl as cl
import pyopencl.clrandom as clrand
from pyopencl.reduction import ReductionKernel
import numpy as np


ctx = cl.create_some_context()
queue = cl.CommandQueue(ctx)

n = 10 ** 7
x = clrand.rand(queue, n, np.float32)

rknl = ReductionKernel(ctx, np.float32, 
    neutral="0",
    reduce_expr="a+b", map_expr="x[i]*x[i]",
    arguments="double *x")

result = rknl(x)
result_np = result.get()