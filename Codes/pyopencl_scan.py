import numpy as np 
import pyopencl as cl
import pyopencl.clrandom as clrand
from pyopencl.scan import GenericScanKernel

# np.cumsum([1, 2, 3])
# np.array([1, 3, 6])

ctx = cl.create_some_context()
queue = cl.CommandQueue(ctx)
print("queue: ", queue)
print()

sknl = GenericScanKernel(ctx, np.float64, 
        arguments="double *y, double *x", 
        input_expr="x[i]",
        scan_expr="a+b", 
        neutral="0",
        output_statement="y[i] = item;")

n = 10 ** 7
x = clrand.rand(queue, n, np.float64)
print("x:", x)
print()

result = cl.array.empty_like(x)
# result = cl.array.arange(queue, n, dtype=np.float64)
sknl(result, x, queue=queue)
print("result", result)
print()

result_np = result.get()
print("result_np", result.get())