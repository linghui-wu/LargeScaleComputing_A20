import pyopencl as cl
import pyopencl.array as cl_array
import numpy as np

ctx = cl.create_some_context()
queue = cl.CommandQueue(ctx)

a = np.random.rand(50000).astype(np.float32)
a_dev = cl_array.to_device(queue, a)

# Double all entries on GPU
twice = 2 * a_dev

# Turn back into Numpy Array
twice_a = twice.get()