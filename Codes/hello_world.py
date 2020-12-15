# Print out "Hello World" on different processors
from mpi4py import MPI
comm = MPI.COMM_WORLD  # COMM_WORLD designation so all available processors communicate with each other 
rank = comm.Get_rank()
size = comm.Get_size()

print("Hello World from rank", rank, "out of ", size, "processors.")