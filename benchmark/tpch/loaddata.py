import dask.dataframe as dd


"""
Loads a table file as generated by TPC-H's dbgen.
Returns an uncomputed dataframe - user must persist if desired.

`path` can be a single path or a glob path, and can be local or an S3 url.
https://docs.dask.org/en/latest/dataframe-api.html#dask.dataframe.read_table
"""

def load_part(path):
    print(path)
    part_df = dd.read_table(
        path,
        sep='|',
        names=[
            'p_partkey',
            'p_name',
            'p_mfgr',
            'p_brand',
            'p_type',
            'p_size',
            'p_container',
            'p_retailprice',
            'p_comment',
            '(empty)',
        ],
    )
    return part_df


def load_supplier(path):
    supplier_df = dd.read_table(
        path,
        sep='|',
        names=[
            's_suppkey',
            's_name',
            's_address',
            's_nationkey',
            's_phone',
            's_acctbal',
            's_comment',
            '(empty)',
        ],
    )
    return supplier_df

def load_partsupp(path):
    partsupp_df = dd.read_csv(
        path,
        sep = '|',
        names = [
            'ps_partkey',
            'ps_suppkey',
            'ps_availqty',
            'ps_supplycost',
            'ps_comment',
            '(empty)',
        ],
    )
    return partsupp_df

def load_customer(path):
    customer_df = dd.read_table(
        path,
        sep='|',
        names=[
            'c_custkey',
            'c_name',
            'c_address',
            'c_nationkey',
            'c_phone',
            'c_acctbal',
            'c_mktsegment',
            'c_comment',
            '(empty)',
        ],
    )
    return customer_df

def load_orders(path):
    orders_df = dd.read_table(
        path,
        sep='|',
        names=[
            'o_orderkey',
            'o_custkey',
            'o_orderstatus',
            'o_totalprice',
            'o_orderdate',
            'o_orderpriority',
            'o_clerk',
            'o_shippriority',
            'o_comment',
            '(empty)',
        ],
    )
    return orders_df

def load_lineitem(path):
    lineitem_df = dd.read_table(
        path,
        sep='|',
        names=[
            'l_orderkey',
            'l_partkey',
            'l_suppkey',
            'l_linenumber',
            'l_quantity',
            'l_extendedprice',
            'l_discount',
            'l_tax',
            'l_returnflag',
            'l_linestatus',
            'l_shipdate',
            'l_commitdate',
            'l_receiptdate',
            'l_shipinstruct',
            'l_shipmode',
            'l_comment',
            '(empty)',
        ],
        # blocksize= 16 * 1024 * 1024,  # 64 MB?
        # parse_dates = [
        #     'l_shipdate',
        #     'l_commitdate',
        #     'l_receiptdate',
        # ],
        # infer_datetime_format=True,
    )

    return lineitem_df

def load_nation(path):
    nation_df = dd.read_table(
        path,
        sep='|',
        names=[
            'n_nationkey',
            'n_name',
            'n_regionkey',
            'n_comment',
            '(empty)',
        ],
    )
    return nation_df

def load_region(path):
    region_df = dd.read_table(
        path,
        sep='|',
        names=[
            'r_regionkey',
            'r_name',
            'r_comment',
            '(empty)',
        ],
    )
    return region_df

# A helper for loading by tablename string.
loader = {
    'PART': load_part,
    'SUPPLIER': load_supplier,
    'PARTSUPP': load_partsupp,
    'CUSTOMER': load_customer,
    'ORDERS': load_orders,
    'LINEITEM': load_lineitem,
    'NATION': load_nation,
    'REGION': load_region,
}
