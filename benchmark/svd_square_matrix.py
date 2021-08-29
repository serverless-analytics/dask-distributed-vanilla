#!/usr/bin/python3

import dask.array as da

from dask.distributed import Client
from dask import delayed

import time
import timeit
import datetime


scheduler = 'vanilla'
name = f'svdsquarematrix16kx4K'
client = Client('10.255.23.115:8786', name=name)


matrix_dim = (16000, 16000)
chunk_dim = (4000, 4000)


# Compute the SVD of 'Tall-and-Skinny' Matrix 
X = da.random.random(matrix_dim, chunks=chunk_dim)
u, s, v = da.linalg.svd_compressed(X, k=5)


start = datetime.datetime.now()
# Start the computation.
v.compute(scheduler='distributed')

end = datetime.datetime.now()

print(f'SVD Square matrix is done in {end - start}')

#v.visualize(filename=f'{name}.png')

