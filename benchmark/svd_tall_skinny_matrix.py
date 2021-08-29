#!/usr/bin/python3

import dask.array as da

from dask.distributed import Client
from dask import delayed

import time
import timeit
import datetime
import sys

scheduler = 'vanilla'
name = f'svd_tall_skinny_matrix_1M_x_4k_{scheduler}'

client = Client('10.255.23.115:8786', name = name)


# Compute the SVD of 'Tall-and-Skinny' Matrix

matrix_dim = (1048576, 4096)
chunk_dim = (16384, 4096)

X = da.random.random(matrix_dim, chunks=chunk_dim)
u, s, v = da.linalg.svd(X)

# Start the computation.
start = datetime.datetime.now()
results = v.compute(scheduler='distributed')
end = datetime.datetime.now()

print(f'SVD tall matrix is done in {end - start}')

#v.visualize(filename=f'{name}.png')



