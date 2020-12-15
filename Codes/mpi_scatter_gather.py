from mpi4py import MPI
import numpy as np

comm = MPI.COMM_WORLD
size = comm.Get_size()  # size = 4
rank = comm.Get_rank()  # rank = 0, 1, 2, 3


###############################################################################
###                     comm.scatter() & comm.gather()                      ###
###############################################################################

if rank == 0:
    data = [(x + 1) ** x for x in range(size)]
    print("We will be scattering:", data)
else:
    data = None

data = comm.scatter(data, root=0)  # Scatter data from root 0
print("rank", rank, "has data:", data)

data += 1

newData = comm.gather(data, root=0)  # Gather data to root 0
if rank == 0:
    print("Master collected:", newData)

###############################################################################
###                            comm.Gather()                                ###
###############################################################################

# sendbuf = np.zeros(100, dtype="int") + rank
# print("sendbuf: ", sendbuf)

# recvbuf = None

# if rank == 0:
#     recvbuf = np.empty([size, 100], dtype="int")
# comm.Gather(sendbuf, recvbuf, root=0)
# print("recvbuf: ", recvbuf)

