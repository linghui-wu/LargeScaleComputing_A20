from mpi4py import MPI
import numpy as np

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

# Send the numpy array from rank 0 to rank 1
if rank == 0:
    data = np.arange(100, dtype='i')
else:
    data = np.empty(100, dtype='i')

# `Bcast` is fast designed for `NumPy`, `bcast` is slow designed for all obj.
comm.Bcast(data, root=0)  

