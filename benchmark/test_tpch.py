import time
import sys
import dask

from dask.distributed import (
    wait,
    futures_of,
    Client,
)

from tpch import loaddata, queries
#from benchmarks import utils

# Paths or URLs to the TPC-H tables.
#table_paths = {
#        'CUSTOMER': 'hdfs://bu-23-115:9000/tpch/customer.tbl',
#    'LINEITEM': 'hdfs://bu-23-115:9000/tpch/lineitem.tbl',
#    'NATION': 'hdfs://bu-23-115:9000/tpch/nation.tbl',
#    'ORDERS': 'hdfs://bu-23-115:9000/tpch/orders.tbl',
#    'PART': 'hdfs://bu-23-115:9000/tpch/part.tbl',
#    'PARTSUPP': 'hdfs://bu-23-115:9000/tpch/partsupp.tbl',
#    'REGION': 'hdfs://bu-23-115:9000/tpch/region.tbl',
#    'SUPPLIER': 'hdfs://bu-23-115:9000/tpch/supplier.tbl',
#}

table_paths = {
    'CUSTOMER': '/root/2g/customer.tbl',
    'LINEITEM': '/root/2g/lineitem.tbl',
    'NATION': '/root/2g/nation.tbl',
    'ORDERS': '/root/2g/orders.tbl',
    'PART': '/root/2g/part.tbl',
    'PARTSUPP': '/root/2g/partsupp.tbl',
    'REGION': '/root/2g/region.tbl',
    'SUPPLIER': '/root/2g/supplier.tbl',
}

#table_paths = {
#    'CUSTOMER': 'https://gochaudhstorage001.blob.core.windows.net/tpch/customer.tbl',
#    'LINEITEM': 'https://gochaudhstorage001.blob.core.windows.net/tpch/lineitem.tbl',
#    'NATION': 'https://gochaudhstorage001.blob.core.windows.net/tpch/nation.tbl',
#    'ORDERS': 'https://gochaudhstorage001.blob.core.windows.net/tpch/orders.tbl',
#    'PART': 'https://gochaudhstorage001.blob.core.windows.net/tpch/part.tbl',
#    'PARTSUPP': 'https://gochaudhstorage001.blob.core.windows.net/tpch/partsupp.tbl',
#    'REGION': 'https://gochaudhstorage001.blob.core.windows.net/tpch/region.tbl',
#    'SUPPLIER': 'https://gochaudhstorage001.blob.core.windows.net/tpch/supplier.tbl',
#}

def main():
    if len(sys.argv) < 2:
        print("args: <dask client>")
        return 1
    client = Client(sys.argv[1])
    timing_supported = False
        # set to False if running against upstream dask without our code changes.

    benchmarker = TpchBenchmarkManager(client, timing_supported)
    benchmarker.load_tables(
        part_path = table_paths['PART'],
        supplier_path = table_paths['SUPPLIER'],
        partsupp_path = table_paths['PARTSUPP'],
        customer_path = table_paths['CUSTOMER'],
        orders_path = table_paths['ORDERS'],
        lineitem_path = table_paths['LINEITEM'],
        nation_path = table_paths['NATION'],
        region_path = table_paths['REGION'],
    )

    # Choose what queries you want to run here.
    benchmarker.run_query(1)
    #benchmarker.run_query(3)
    #benchmarker.run_query(6)
    #benchmarker.run_query(21)


class TpchBenchmarkManager:
    def __init__(self, client, timing_supported=True):
        self.client = client
        self.timing_supported = timing_supported
        self.tables = {}

    def load_tables(self,
        *,
        # Paths/URLs for TPCH tables source data.
        part_path=None,
        supplier_path=None,
        partsupp_path=None,
        customer_path=None,
        orders_path=None,
        lineitem_path=None,
        nation_path=None,
        region_path=None,
    ):
        paths = {
            'PART': part_path,
            'SUPPLIER': supplier_path,
            'PARTSUPP': partsupp_path,
            'CUSTOMER': customer_path,
            'ORDERS': orders_path,
            'LINEITEM': lineitem_path,
            'NATION': nation_path,
            'REGION': region_path,
        }

        for tablename, path in paths.items():

            if path is None:
                print("\nNo path given for table {}. Skipping.".format(tablename))
                continue

            print("\n====================================")
            print("Ingesting table {}... \n(from {})".format(tablename, path))
            load_start = time.time()
            table = loaddata.loader[tablename](path)
            #table = self.client.persist(table)
            #wait(table)
            load_duration = time.time() - load_start
            self.tables[tablename] = table
            futures = futures_of(table)
            print("...complete.")
            print("\nE2E time: {:.3f} seconds. Number of partitions: {}".format(
                load_duration, len(futures)))
            print("====================================\n")


            if self.timing_supported:
                longest_future = None
                longest_future_duration = None
                for future in futures:
                    duration = self.client.timing_info(future)[0]['duration']
                    if longest_future is None or duration > longest_future_duration:
                        longest_future = future
                        longest_future_duration = duration
                print("Profile of slowest partition:")
                #utils.prettyprint_timing_info(self.client.timing_info(longest_future))

    def run_query(self, query_num):
        print("\n====================================")
        print("Executing query {}...".format(query_num))
        query_start = time.time()
        futures = queries.by_number[query_num](self.tables)
        future = self.client.compute(futures)
        result = self.client.gather(future)
        query_duration = time.time() - query_start

        print("...complete.")
        print("\nE2E time: {:.3f} seconds.".format(query_duration))
        if self.timing_supported:
            try:
                utils.prettyprint_timing_info(self.client.timing_info(future))
            except Exception as e:
                print(str(e))
        print(result)
        return future




if __name__ == '__main__':
    main()
