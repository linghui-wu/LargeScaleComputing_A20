import pyopencl as cl
import numpy as np

# platform = cl.get_platforms()[0]
# device = platform.get_devices()[0]  
# ctx = cl.Context([device])

ctx = cl.create_some_context()
queue = cl.CommandQueue(ctx)

a = np.random.rand(50000).astype(np.float32) 
a_buf = cl.Buffer(ctx, cl.mem_flags.READ_WRITE, size=a.nbytes) 
cl.enqueue_copy(queue, a_buf, a)

prg = cl.Program(ctx,
    """
    __kernel void twice(__global float *a)
    {
        int gid = get_global_id(0);
        a[gid] = 2 * a[gid];
    }
    """).build()

prg.twice(queue, a.shape, None, a_buf)

result = np.empty_like(a)
cl.enqueue_copy(queue, result, a_buf)