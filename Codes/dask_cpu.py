import cudf

df = cudf.read_csv("myfile.csv")
df = df[df.name == "Alice"]
df.groupby("id").value.mean()

from dask_cuda import LocalCUDACluster
import dask_cudf
from dask.distributed import Client

cluster = LocalCUDACluster()
client = Client(cluster)

gdf = dask_cudf.read_csv("data/nyc-taxi/*.csv")
gdf.passenger_count.sum().compute()