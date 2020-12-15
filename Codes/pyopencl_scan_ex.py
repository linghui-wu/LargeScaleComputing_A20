import numpy as np
import pyopencl as cl
from pyopencl.scan import GenericScanKernel

ctx = cl.create_some_context()
queue = cl.CommandQueue(ctx)

a = cl.array.arange(queue, 400, dtype=np.float32)
b = cl.array.arange(queue, 400, dtype=np.float32)

knl = GenericScanKernel(
        ctx, np.int32,
        arguments="__global int *ary",
        input_expr="ary[i]",
        scan_expr="a+b", neutral="0",
        output_statement="ary[i+1] = item;")

a = cl.array.arange(queue, 10000, dtype=np.int32)
knl(a, queue=queue)

knl = GenericScanKernel(
        ctx, np.int32,
        arguments="__global int *ary, __global int *out",
        input_expr="(ary[i] > 300) ? 1 : 0",
        scan_expr="a+b", neutral="0",
        output_statement="""
            if (prev_item != item) out[item-1] = ary[i];
            """)

out = a.copy()
knl(a, out)

a_host = a.get()
out_host = a_host[a_host > 300]

assert (out_host == out.get()[:len(out_host)]).all()