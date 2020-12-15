from mpi4py import MPI
rank = MPI.COMM_WORLD.Get_rank()

if rank == 0:
    data = {'a': 7, 'b': 3.14}
    comm.send(data, dest=1)
elif rank == 2:
    data = comm.recv(source=0)