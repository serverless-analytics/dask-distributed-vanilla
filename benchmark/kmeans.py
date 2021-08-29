#!/usr/bin/python3

import dask.array as da

from dask.distributed import Client
from dask import delayed

import time
import timeit
import datetime
import sys

import dask_ml.datasets
import dask_ml.cluster


name = 'kmeans'

client = Client('10.255.23.115:8786', name = name)


X, y = dask_ml.datasets.make_blobs(n_samples=10000000,
                                   chunks=1000000,
                                   random_state=0,
                                   centers=3)
X = X.persist()

km = dask_ml.cluster.KMeans(n_clusters=3, init_max_iter=2, oversampling_factor=10)
km.fit(X)

