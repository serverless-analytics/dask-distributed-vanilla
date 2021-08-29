import dask.dataframe as dd
import pandas as pd


"""
All queries take in Dask Dataframes,
and return results as an uncomputed dask dataframe.
"""


def query1(tables):
    pd.set_option('float_format', '{:.2f}'.format)

    #######################################################
    # FROM lineitem
    #######################################################
    lineitem = tables['LINEITEM']

    #######################################################
    # WHERE l_shipdate <= date '1998-12-01' - interval '[DELTA]' day (3)
    #######################################################

    # Note that the official TPC-H spec requires DELTA to be randomized
    # between 60-120.
    # Snowflake just sets it to 90:
    # https://docs.snowflake.com/en/user-guide/sample-data-tpch.html
    lineitem = lineitem[(lineitem['l_shipdate'] < '1998-10-01')]


    #######################################################
    # GROUP BY
    #     l_returnflag,
    #     l_linestatus
    #######################################################
    # SELECT
    #     l_returnflag,
    #     l_linestatus,
    #     sum(l_quantity) as sum_qty,
    #     sum(l_extendedprice) as sum_base_price,
    #     sum(l_extendedprice * (1-l_discount)) as sum_disc_price,
    #     sum(l_extendedprice * (1-l_discount) * (1+l_tax)) as sum_charge,
    #     avg(l_quantity) as avg_qty,
    #     avg(l_extendedprice) as avg_price,
    #     avg(l_discount) as avg_disc,
    #     count(*) as count_order
    #######################################################

    # Pre-computing columns for multi-column aggregation.
    # (I could not find a pandas API for multi-column aggregation,
    # so this is the cleanest alternative I could think of.)
    lineitem['disc_price'] = (
        lineitem['l_extendedprice'] * (1 - lineitem['l_discount']))
    lineitem['charge'] = (
        lineitem['disc_price'] * (1 + lineitem['l_tax']))
    # Do the groupby (this also sorts on the groups)
    lineitem = lineitem.groupby(['l_returnflag', 'l_linestatus'])
    # NamedAgg does not seem to be supported in uncomputed dask dataframes,
    # so we will do aggregates and then rename the columns.
    lineitem = lineitem.agg({
        'l_quantity': ['sum', 'mean'],
        'l_extendedprice': ['sum', 'mean'],
        'disc_price': 'sum',
        'charge': 'sum',
        'l_discount': 'mean',
        'l_orderkey': 'count',
    })
    # Renaming columns.
    lineitem.columns = [
        'sum_qty',
        'avg_qty',
        'sum_base_price',
        'avg_price',
        'sum_disc_price',
        'sum_charge',
        'avg_disc',
        'count_order',
    ]
    # Reordering columns.
    lineitem = lineitem[[
        'sum_qty',
        'sum_base_price',
        'sum_disc_price',
        'sum_charge',
        'avg_qty',
        'avg_price',
        'avg_disc',
        'count_order',
    ]]

    #####################
    # ORDER BY
    #     l_returnflag,
    #     l_linestatus;
    #####################
    # This is already done during the aggregation.

    return lineitem


def query2(tables):
    pd.set_option('float_format', '{:.2f}'.format)
    region = tables['REGION']
    nation = tables['NATION']
    supplier = tables['SUPPLIER']
    partsupp = tables['PARTSUPP']
    part = tables['PART']


    europe = region[region['r_name'] == 'EUROPE'].\
            merge(nation, how='inner', on = None,
                left_on = 'r_regionkey', right_on = 'n_regionkey',
                left_index=False, right_index=False).\
            merge(supplier, how='inner', on = None,
                left_on = 'n_nationkey', right_on = 's_nationkey',
                left_index=False, right_index=False).\
            merge(partsupp, how='inner', on = None,
                left_on = 's_suppkey', right_on = 'ps_suppkey',
                left_index=False, right_index=False)

    brass = part[(part['p_size'] == 15) & (part['p_type'].str.endswith('BRASS'))].\
            merge(europe, how='inner', on=None,
                left_on = 'p_partkey', right_on = 'ps_partkey',
                left_index=False, right_index=False)

    minCost = brass.groupby('ps_partkey').\
            agg({'ps_supplycost': 'min'}).\
            reset_index(drop=False)
    minCost['min'] = minCost['ps_supplycost']

    minBrass = brass.merge(minCost, how='inner', on=None,
                left_on = 'ps_partkey', right_on = 'ps_partkey',
                left_index=False, right_index=False)#\
#            [["s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment"]]
    result = minBrass[minBrass['ps_supplycost_y'] == minBrass['min']]
    return result


def query3(tables):
    pd.set_option('float_format', '{:.2f}'.format)

    ##############################
    # FROM
    #   customer,
    #   orders,
    #   lineitem
    ##############################

    customer = tables['CUSTOMER']
    orders = tables['ORDERS']
    lineitem = tables['LINEITEM']


    #######################################
    # WHERE c_mktsegment = 'BUILDING'
    #   AND o_orderdate < '1995-03-15'
    #   AND l_shipdate > '1995-03-15'
    #   ...
    ######################################

    # Do the selections.
    customer = customer[(customer['c_mktsegment'] == 'BUILDING')]
    orders = orders[(orders['o_orderdate'] < '1995-03-15')]
    lineitem = lineitem[(lineitem['l_shipdate'] > '1995-03-15')]


    ######################################
    #   ...
    #   AND c_custkey = o_custkey
    #   AND l_orderkey = o_orderkey
    ######################################

    # Do the join.

    o_c = orders.join(
        customer.set_index('c_custkey'), on='o_custkey', lsuffix='_o', rsuffix='_c')
    l_o_c = lineitem.join(
        o_c.set_index('o_orderkey'), on='l_orderkey')

    #############################
    # GROUP BY
    #   l_orderkey,
    #   o_orderdate,
    #   o_shippriority
    #############################
    # SELECT
    #   l_orderkey,
    #   sum(l_extendedprice * (1 - l_discount)) as revenue,
    #   o_orderdate,
    #   o_shippriority
    #############################

    # Precomputing columns for aggregation.
    l_o_c['revenue_summands'] = (
        l_o_c['l_extendedprice'] * (1 - l_o_c['l_discount']))

    # GROUP BY
    l_o_c = l_o_c.groupby([
        'l_orderkey',
        'o_orderdate',
        'o_shippriority',
    ])
    # Aggregate
    l_o_c = l_o_c.agg({
        'revenue_summands': 'sum',
    })
    # Rename output column
    l_o_c.columns = [
        'revenue',
    ]

    ######################
    # ORDER BY
    #   revenue desc,
    #   o_orderdate;  # <-- NOT IMPLEMENTED
    # LIMIT 10
    ######################

    # no sort_values in dask dataframes, so we do this:
    # l_o_c = l_o_c.reset_index()
    l_o_c = l_o_c.nlargest(n=10, columns=['revenue'])
    # Unfortunately dask dataframes cannot sort by string (!)
    # so we will not be sorting by the order date.

    return l_o_c


def query4(tables):
    orders = tables['ORDERS']
    lineitem = tables['LINEITEM']
    forders = orders[(orders['o_orderdate'] < "1993-10-01") & (orders['o_orderdate'] >= "1993-07-01")]

    flineitems = lineitem[lineitem['l_commitdate'] < 'l_receiptdate']#['l_orderkey']#.unique()
    result = forders.merge(flineitems, how='inner', on=None,
            left_on='o_orderkey', right_on='l_orderkey',
            left_index=False, right_index=False, suffixes=("_left", None)).\
            groupby('o_orderpriority').agg({'o_orderpriority': 'count'})

    return result


def query5(tables):
    orders = tables['ORDERS']
    region = tables['REGION']
    nation = tables['NATION']
    supplier = tables['SUPPLIER']
    lineitem = tables['LINEITEM']
    customer = tables['CUSTOMER']

    forders = orders[(orders['o_orderdate'] < "1995-01-01") & (orders['o_orderdate'] >= "1994-01-01")]
    
    fregion = region[region['r_name'] == 'ASIA'].\
            merge(nation, how='inner', on = None, 
                left_on = 'r_regionkey', right_on = 'n_regionkey',
                left_index=False, right_index=False).\
            merge(supplier, how='inner', on = None,
                left_on='n_nationkey', right_on='s_nationkey',
                left_index=False, right_index=False).\
            merge(lineitem, how='inner', on = None,
                left_on='s_suppkey', right_on='l_suppkey',
                left_index=False, right_index=False)\
            [['n_name', 'l_extendedprice', 'l_discount', 'l_orderkey', 's_nationkey']].\
            merge(forders, how='inner', on = None,
                left_on='l_orderkey', right_on='o_orderkey',
                left_index=False, right_index=False).\
            merge(customer, how='inner', on = None,
                left_on='o_custkey', right_on='c_custkey',
                left_index=False, right_index=False).\
            merge(customer, how='inner', on = None,
                left_on='s_nationkey', right_on='c_nationkey',
                left_index=False, right_index=False)
    
    fregion['value'] = fregion['l_extendedprice']*(1 - fregion['l_discount'])

    revenue = fregion.groupby(['n_name']).agg({'value': 'sum'}).reset_index(drop=False)

    return revenue



def query6(tables):
    pd.set_option('float_format', '{:.2f}'.format)

    #######################################################
    # FROM lineitem
    #######################################################
    lineitem = tables['LINEITEM']

    #######################################################
    # WHERE l_shipdate >= date '[DATE]'
    # AND   l_shipdate < date '[DATE]' + interval '1' year
    # AND   l_discount between [DISCOUNT] - 0.01 and [DISCOUNT] + 0.01
    # AND   l_quantity < [QUANTITY]
    #######################################################

    # For now, use validation parameters for randomized variables:
    # DATE = 1994-01-01
    # DISCOUNT = 0.06
    # QUANTITY = 24

    lineitem = lineitem[
        (lineitem['l_shipdate'] >= '1994-01-01')
        & (lineitem['l_shipdate'] < '1995-01-01')
        & (lineitem['l_discount'] >= 0.05)
        & (lineitem['l_discount'] <= 0.07)
        & (lineitem['l_quantity'] < 24)
    ]

    #######################################################
    # SELECT sum(l_extendedprice * l_discount) as revenue
    #######################################################
    lineitem['price_x_discount'] = (
        lineitem['l_extendedprice'] * lineitem['l_discount']
    )
    # Note that this returns a scalar value
    # instead of a single-cell DF.
    # Dask dataframes do not support ungrouped aggregations
    # (or making a vacuous group).
    lineitem = lineitem[['price_x_discount']]
    lineitem = lineitem.sum()

    # Wrapping result in a dataframe.
    lineitem = lineitem.to_frame()
    lineitem.columns = ['revenue']
    lineitem = lineitem[['revenue']]
    lineitem = lineitem.reset_index(drop=True)

    return lineitem

def query7(tables):
    pd.set_option('float_format', '{:.2f}'.format)

    getYear = lambda x : x[0:4]
    decrease = lambda x, y : x*(1-y)

    #######################################################
    # FROM SUPPLIER, LINEITEM, ORDERS, CUSTOMER, NATION N1, NATION N2
    #######################################################
    lineitem = tables['LINEITEM']
    nation = tables['NATION']
    supplier = tables['SUPPLIER']
    customer = tables['CUSTOMER']
    order = tables['ORDERS']

    fnation = nation[
            (nation['n_name'] == "FRANCE") |
            (nation['n_name'] == "GERMANY")]
    fline = lineitem[
            (lineitem['l_shipdate'] >= "1995-01-01") &
            (lineitem['l_shipdate'] <= "1996-12-31")]


    supNation = fnation.merge(supplier, how='inner', on = None,
            left_on="n_nationkey", right_on='s_nationkey',
            left_index=False, right_index=False).merge(fline, how='inner', on = None, 
            left_on='s_suppkey', right_on='l_suppkey',
            left_index=False, right_index=False)[["n_name", "l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"]]
    supNation = supNation.rename(columns={"n_name": "supp_nation"})


    cusNation = fnation.merge(customer, how='inner', on = None,
            left_on="n_nationkey", right_on='c_nationkey',
            left_index=False, right_index=False).merge(order, how='inner', on = None,
            left_on="c_custkey", right_on='o_custkey',
            left_index=False, right_index=False)[["n_name", "o_orderkey"]]
    cusNation = cusNation.rename(columns={"n_name": "cust_nation"})


    cusSupNation = cusNation.merge(supNation, how='inner', on=None,
            left_on="o_orderkey", right_on='l_orderkey',
            left_index=False, right_index=False)
    cusSupNation['volume'] = cusSupNation['l_extendedprice']*(1 - cusSupNation['l_discount']) 
    cusSupNation['l_year'] = cusSupNation['l_shipdate'].str[:4]

    cusSupNation1 = cusSupNation[\
            ((cusSupNation['supp_nation'] == 'FRANCE') & (cusSupNation['cust_nation'] == 'GERMANY')) \
            | ((cusSupNation['supp_nation'] == 'GERMANY') & (cusSupNation['cust_nation'] == 'FRANCE'))]\
            [['supp_nation', 'cust_nation', 'l_year', 'volume']]

    # .groupBy($"supp_nation", $"cust_nation", $"l_year")
    revenue = cusSupNation1.groupby(['supp_nation', 'cust_nation', 'l_year']).agg({'volume': 'sum'}).reset_index(drop=False)

    return revenue


def query8(tables):
    #val fregion = region.filter($"r_name" === "AMERICA")
    lineitem = tables['LINEITEM']
    nation = tables['NATION']
    supplier = tables['SUPPLIER']
    customer = tables['CUSTOMER']
    order = tables['ORDERS']
    part = tables['PART']
    region = tables['REGION']
    
    fregion = region[region['r_name'] == "AMERICA"]
    forders = order[(order['o_orderdate'] <= "1996-12-31") & (order['o_orderdate'] >= "1995-01-01")]
    fpart = part[part['p_type'] == 'ECONOMY ANODIZED STEEL']
    
    nat = nation.merge(supplier, how='inner', on = None,
            left_on="n_nationkey", right_on='s_nationkey',
            left_index=False, right_index=False, suffixes=("_left", None))


    lineitem['volume'] = (1 - lineitem['l_discount'])*lineitem['l_extendedprice']
    line = lineitem[['l_partkey', 'l_suppkey', 'l_orderkey', 'volume']].\
            merge(fpart, how='inner', on = None,
                left_on="l_partkey", right_on='p_partkey',
                left_index=False, right_index=False, suffixes=("_left", None)).\
            merge(nat, how='inner', on = None,
                left_on="l_suppkey", right_on='s_suppkey',
                left_index=False, right_index=False, suffixes=("_left", None))


    natLine = nation.merge(fregion, how='inner', on = None,
                left_on="n_regionkey", right_on='r_regionkey',
                left_index=False, right_index=False, suffixes=("_left", None)).\
            merge(customer, how='inner', on = None,
                left_on="n_nationkey", right_on='c_nationkey',
                left_index=False, right_index=False, suffixes=("_left", None)).\
            merge(forders, how='inner', on = None,
                left_on="c_custkey", right_on='o_custkey',
                left_index=False, right_index=False, suffixes=("_left", None)).\
            merge(line, how='inner', on = None,
                left_on="o_orderkey", right_on='l_orderkey',
                left_index=False, right_index=False, suffixes=("_left", None))

    natLine['o_year'] = natLine['o_orderdate'].str[0:4]
    natLine['case_volume'] = natLine['volume']
    natLine[natLine['n_name'] != 'BRAZIL']['case_volume'] = 0


    result = natLine.groupby('o_year').agg({'case_volume': 'sum', 'volume' : 'sum'})
    return result


def query9(tables):
    lineitem = tables['LINEITEM']
    nation = tables['NATION']
    supplier = tables['SUPPLIER']
    #customer = tables['CUSTOMER']
    order = tables['ORDERS']
    part = tables['PART']
    partsupp = tables['PARTSUPP']
    #region = tables['REGION']

    linePart = part[part['p_name'].str.contains('green')].\
            merge(lineitem, how='inner', on = None,
                left_on ='p_partkey', right_on='l_partkey',
                left_index=False, right_index=False, suffixes=("_left", None))

    natSup = nation.merge(supplier, how='inner',
            left_on ='n_nationkey', right_on='s_nationkey',
            left_index=False, right_index=False, suffixes=("_left", None))

    lineOrder = linePart.merge(natSup, left_on ='l_suppkey', right_on='s_suppkey').\
            merge(partsupp, left_on=['l_suppkey', 'l_partkey'], 
                right_on=['ps_suppkey', 'ps_partkey']).\
            merge(order, left_on='l_orderkey', right_on='o_orderkey')

    lineOrder['o_year'] = lineOrder['o_orderdate'].str[0:4]
    lineOrder['amount'] = (1 - lineOrder['l_discount'])*lineOrder['l_extendedprice'] - (lineOrder['ps_supplycost']*lineOrder['l_quantity'])
    result = lineOrder[['n_name', 'o_year', 'amount']].\
            groupby(['n_name', 'o_year']).\
            agg({'amount' : 'sum'}).\
            reset_index(drop= False)

    return result


def query10(tables):
    lineitem = tables['LINEITEM']
    nation = tables['NATION']
    customer = tables['CUSTOMER']
    order = tables['ORDERS']

    flineitem = lineitem[lineitem['l_returnflag'] == 'R']
    forder = order[(order['o_orderdate'] < "1994-01-01") & (order['o_orderdate'] >= "1993-10-01")]
    lineOrder = forder.merge(customer, left_on ='o_custkey', right_on='c_custkey').\
        merge(nation, left_on ='c_nationkey', right_on='n_nationkey').\
        merge(flineitem, left_on ='o_orderkey', right_on='l_orderkey')

    lineOrder['volume'] = (1 - lineOrder['l_discount'])*lineOrder['l_extendedprice']
    result = lineOrder[['c_custkey', 'c_name', 'volume', 'c_acctbal', 'n_name', 'c_address', 'c_phone', 'c_comment']].\
            groupby(['c_custkey', 'c_name', 'c_acctbal', 'c_phone', 'n_name', 'c_address', 'c_comment']).\
            agg({'volume': 'sum'}).reset_index(drop=False)

    result['revenue'] = result['volume']
    result = result.nlargest(20, 'revenue')
    return result; 


def query11(tables):
    nation = tables['NATION']
    supplier = tables['SUPPLIER']
    partsupp = tables['PARTSUPP']

    natSup = nation[nation['n_name']== "GERMANY"].\
            merge(supplier, left_on='n_nationkey', right_on='s_nationkey').\
            merge(partsupp, left_on='s_suppkey', right_on='ps_suppkey')
    natSup['value'] = natSup['ps_supplycost']*natSup['ps_availqty']
    
    tmp = natSup[['ps_partkey', 'value']]
    
    tmp['total_value'] = natSup['value'].sum()
    tmp['mul01'] = tmp['total_value']*0.0001
    sumRes = tmp.groupby('ps_partkey').agg({'value': 'sum'}).reset_index(drop=False)
    sumRes['part_value'] = sumRes['value']
    result = tmp.merge(sumRes, left_on='ps_partkey', right_on='ps_partkey', suffixes=(None, '_right'))
    return result[result['part_value'] > result['mul01']][['ps_partkey', 'value']]



def query12(tables):
    lineitem = tables['LINEITEM']
    order = tables['ORDERS']

    flineitem = lineitem[
            ((lineitem['l_shipmode'] == 'MAIL') | (lineitem['l_shipmode'] == 'SHIP')) &\
            (lineitem['l_commitdate'] < lineitem['l_receiptdate']) &\
            (lineitem['l_shipdate'] < lineitem['l_commitdate']) &\
            (lineitem['l_receiptdate'] >= '1994-01-01') &\
            (lineitem['l_receiptdate'] < '1995-01-01')]
    
    lineorder = flineitem.merge(order, left_on='l_orderkey',  right_on='o_orderkey')

    lineorder['highPriority'] = lineorder['o_orderpriority'].where((lineorder['o_orderpriority'] == '1-URGENT') | (lineorder['o_orderpriority'] == '2-HIGH'), 0)
    lineorder['highPriority'] = lineorder['highPriority'].where(lineorder['highPriority'] == 0, 1) 


    lineorder['lowPriority'] = lineorder['o_orderpriority'].where((lineorder['o_orderpriority'] != '1-URGENT') & (lineorder['o_orderpriority'] != '2-HIGH'), 0)
    lineorder['lowPriority'] = lineorder['lowPriority'].where(lineorder['lowPriority'] == 0, 1) 

    result = lineorder.groupby('l_shipmode').agg({'highPriority': 'sum', 'lowPriority':'sum'}).\
            reset_index(drop=False)
            
    result['sum_highorderpriority'] = result['highPriority']  
    result['sum_loworderpriority'] = result['lowPriority'] 
    
    return result


def query13(tables):
    customer = tables['CUSTOMER']
    order = tables['ORDERS']

    order['nonspecial'] = order['o_comment'].where(order['o_comment'].str.contains("special") & order['o_comment'].str.contains("requests"), 1)
    order['nonspecial'] = order['nonspecial'].where(order['nonspecial'] == 1, 0)
    custOrder = customer.merge(order[order['nonspecial'] == 1], left_on='c_custkey', right_on='o_custkey').\
            groupby('o_custkey').agg({'o_orderkey' : 'count'}).reset_index(drop=False)
    custOrder['c_count'] = custOrder['o_orderkey']
    result = custOrder.groupby('c_count').agg({'o_custkey': 'count'}).reset_index(drop=False)
    result['custdist'] = result['o_custkey']
    return result


def query14(tables):
    part = tables['PART']
    lineitem = tables['LINEITEM']
    partLine = part.merge(lineitem[(lineitem['l_shipdate'] > '1995-09-01') & (lineitem['l_shipdate'] < '1995-10-01')], 
            left_on='p_partkey', right_on='l_partkey')\
            [['p_type', 'l_extendedprice', 'l_discount']]
    partLine['value'] = partLine['l_extendedprice']*(1 - partLine['l_discount'])
    partLine['promo'] = partLine['value']
    partLine[~partLine['p_type'].str.startswith('PROMO')]['promo'] = 0
    result = partLine['promo'].sum()*100/partLine['value'].sum()
    return result


def query15(tables):
    lineitem = tables['LINEITEM']
    supplier = tables['SUPPLIER']
    flineitem = lineitem[(lineitem['l_shipdate'] >= '1996-01-01') & (lineitem['l_shipdate'] < '1996-04-01')]
    flineitem['value'] = flineitem['l_extendedprice']*(1 - flineitem['l_discount'])
    revenue = flineitem.groupby('l_suppkey').agg({'value': 'sum'}).reset_index(drop=False)
    revenue['total'] = revenue['value']
    revenue['max_total'] = revenue['total'].max()
    result = revenue[revenue['total'] == revenue['max_total']].\
            merge(supplier, left_on='l_suppkey', right_on='s_suppkey')\
            [['s_suppkey', 's_name', 's_address', 's_phone', 'total']].set_index('s_suppkey').reset_index(drop=False)
    return revSup


def query16(tables):
    part = tables['PART']
    supplier = tables['SUPPLIER']
    partsupp = tables['PARTSUPP']

    part['nonpolished'] = part['p_type'].where(part['p_type'].str.startswith("MEDIUM POLISHED"), 1)
    part['nonpolished'] = part['nonpolished'].where(part['nonpolished'] == 1, 0)
    

    num_math = ["49", "14", "23", "45", "19", "3", "36", "9"]  
    part['number'] = part['p_size'].where(part['p_size'].astype(str).isin(num_math), 0)
    part['number'] = part['number'].where(part['number'] == 0, 1)
    

    supplier['complains'] = supplier['s_comment'].where(supplier['s_comment'].str.contains("Complaints") & supplier['s_comment'].str.contains("Customer"), 0)
    supplier['complains'] = supplier['complains'].where(supplier['complains'] == 0, 1)

    fparts = part[(part['p_brand'] == 'Brand#45') &\
            (part['nonpolished'] == 1) &\
            (part['number'] == 1)][['p_partkey', 'p_brand', 'p_type', 'p_size']] 

    fsupplier = supplier[supplier['complains'] == 0].\
            merge(partsupp, left_on='s_suppkey', right_on='ps_suppkey')[['ps_partkey', 'ps_suppkey']].\
            merge(fparts, left_on='ps_partkey', right_on='p_partkey')

    result = fsupplier.groupby(['p_brand', 'p_type', 'p_size']).agg({'ps_suppkey': 'count'}).reset_index(drop=False);
    result['supplier_count'] = result['ps_suppkey'] 
    return result


def query17(tables):
    lineitem = tables['LINEITEM']
    part = tables['PART']

    flineitem = lineitem[['l_partkey', 'l_quantity', 'l_extendedprice']]

    part_line = part[(part['p_brand'] == 'Brand#23') & (part['p_container'] == 'MED BOX')].\
            merge(lineitem, left_on='p_partkey', right_on='l_partkey', how='left')

    ag_pline = part_line.groupby('p_partkey').agg({'l_quantity': 'mean'}).reset_index(drop=False)

    ag_pline['avg_quantity'] = ag_pline['l_quantity']*0.2
    ag_pline['key'] = ag_pline['p_partkey']

    result = ag_pline[['key', 'avg_quantity']].\
            merge(part_line, left_on='key', right_on='p_partkey')
    result = result[result['l_quantity'] < result['avg_quantity']]['l_extendedprice'].sum()/7

    return result


def query18(tables):
    lineitem = tables['LINEITEM']
    order = tables['ORDERS']
    customer = tables['CUSTOMER']

    linequant = lineitem.groupby('l_orderkey').agg({'l_quantity': 'sum'}).reset_index(drop=False)
    linequant['sum_quantity'] = linequant['l_quantity']
    linequant['key'] = linequant['l_orderkey']
    result = linequant[linequant['sum_quantity']>300]\
            [['key', 'sum_quantity', 'l_quantity']].\
            merge(order, left_on='key', right_on='o_orderkey').\
            merge(customer, left_on='o_custkey', right_on='c_custkey')\
            [['l_quantity', 'c_name', 'c_custkey', 'o_orderkey', 'o_orderdate', 'o_totalprice']].\
            groupby(['c_name', 'c_custkey', 'o_orderkey', 'o_orderdate', 'o_totalprice']).agg({'l_quantity': 'sum'}).reset_index(drop=False)
    return result


def query19(tables):
    lineitem = tables['LINEITEM']
    part = tables['PART']

    part_line = part.merge(lineitem, left_on='p_partkey', right_on='l_partkey')
    part_line = part_line[((part_line['l_shipmode'] == 'AIR') | (part_line['l_shipmode'] == 'AIR REG')) &
            (part_line['l_shipinstruct'] == 'DELIVER IN PERSON')].reset_index(drop=False)

    sm = ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]  
    part_line['sm'] = part_line['p_container'].where(part_line['p_container'].isin(sm), 0)
    part_line['sm'] = part_line['sm'].where(part_line['sm'] == 0, 1)
    
    md = ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]
    part_line['md'] = part_line['p_container'].where(part_line['p_container'].isin(md), 0)
    part_line['md'] = part_line['md'].where(part_line['md'] == 0, 1)

    lg = ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]
    part_line['lg'] = part_line['p_container'].where(part_line['p_container'].isin(lg), 0)
    part_line['lg'] = part_line['lg'].where(part_line['lg'] == 0, 1)

    fpart_line = part_line[\
            ((part_line['p_brand'] == 'Brand#12') &\
            (part_line['sm'] == 1) &\
            (part_line['l_quantity'] >= 1) & (part_line['l_quantity'] <= 11) & \
            (part_line['p_size'] >= 1) & (part_line['p_size'] <= 5)) |
            ((part_line['p_brand'] == 'Brand#23') &\
            (part_line['md'] == 1) &\
            (part_line['l_quantity'] >= 10) & (part_line['l_quantity'] <= 20) & \
            (part_line['p_size'] >= 1) & (part_line['p_size'] <= 10)) |
            ((part_line['p_brand'] == 'Brand#34') &\
            (part_line['lg'] == 1) &\
            (part_line['l_quantity'] >= 20) & (part_line['l_quantity'] <= 30) & \
            (part_line['p_size'] >= 1) & (part_line['p_size'] <= 15))
            ]
   
    fpart_line['volume'] = (1 - fpart_line['l_discount'])*fpart_line['l_extendedprice']
    return fpart_line['volume'].sum()


def query20(tables):
    nation = tables['NATION']
    lineitem = tables['LINEITEM']
    supplier = tables['SUPPLIER']
    part = tables['PART']
    partsupp = tables['PARTSUPP']


    flineitem = lineitem[(lineitem['l_shipdate'] > '1994-01-01') & (lineitem['l_shipdate'] < '1995-01-01')].\
            groupby(['l_partkey', 'l_suppkey']).agg({'l_quantity' : 'sum' }).reset_index()

    flineitem['sum_quantity'] = flineitem['l_quantity']*0.5
    fnation = nation[nation['n_name'] == 'CANADA']
    nat_supp = supplier[['s_suppkey', 's_name', 's_nationkey', 's_address']].\
            merge(fnation, left_on='s_nationkey', right_on = 'n_nationkey')
 
    part_line = part[part['p_name'].str.startswith('forest')]['p_partkey'].unique().reset_index(drop=False).\
            merge(partsupp, left_on='p_partkey', right_on = 'ps_partkey').\
            merge(flineitem, left_on=['ps_suppkey', 'ps_partkey'], right_on=['l_suppkey', 'l_partkey'])

    result = part_line[part_line['ps_availqty'] > part_line['sum_quantity']]['ps_suppkey'].unique().reset_index(drop=False).\
            merge(nat_supp, left_on='ps_suppkey', right_on='s_suppkey')\
            [['s_name', 's_address']].set_index('s_name').reset_index(drop=False)
    return result



def query21(tables):
    supplier = tables['SUPPLIER']
    lineitem = tables['LINEITEM']
    orders = tables['ORDERS']
    nation = tables['NATION']


    fsupplier = supplier[['s_suppkey', 's_nationkey', 's_name']]
    plineitem = lineitem[['l_suppkey', 'l_orderkey', 'l_receiptdate', 'l_commitdate']]
    flineitem = plineitem[plineitem['l_receiptdate'] > plineitem['l_commitdate']]
    forders = orders[orders['o_orderstatus'] == 'F'][['o_orderkey', 'o_orderstatus']]

    line10 = plineitem.groupby('l_orderkey').agg({'l_suppkey': 'count'}).reset_index(drop=False)
    line10['suppkey_count'] = line10['l_suppkey']
    line11 = plineitem.groupby('l_orderkey').agg({'l_suppkey' : 'max'}).reset_index(drop=False)
    line11['suppkey_max'] = line11['l_suppkey']
    line1 = line10.merge(line11)[['l_orderkey', 'suppkey_count', 'suppkey_max']]
    line1['key'] = line1['l_orderkey']

    line20 = flineitem.groupby('l_orderkey').agg({'l_suppkey': 'count'}).reset_index(drop=False)
    line20['suppkey_count'] = line20['l_suppkey']
    line21 = flineitem.groupby('l_orderkey').agg({'l_suppkey' : 'max'}).reset_index(drop=False)
    line21['suppkey_max'] = line21['l_suppkey']
    line2 = line20.merge(line21)[['l_orderkey', 'suppkey_count', 'suppkey_max']]
    line2['key'] = line2['l_orderkey']


    natLine = nation[nation['n_name'] == 'SAUDI ARABIA'].\
            merge(fsupplier, left_on = 'n_nationkey', right_on = 's_nationkey').\
            merge(lineitem, left_on='s_suppkey', right_on='l_suppkey').\
            merge(forders, left_on= 'l_orderkey', right_on='o_orderkey').\
            merge(line1, left_on='l_orderkey', right_on='l_orderkey')

    #result = natLine[(natLine['suppkey_count'] > 1) | ((natLine['suppkey_count'] == 1) & (natLine['l_suppkey'] == 'max_suppkey'))]
    #        [['s_name', 'l_orderkey', 'l_suppkey']].\
    #        merge(line2, left_on='l_orderkey', right_on='key', how='left')\
    #        [['s_name', 'l_orderkey', 'l_suppkey', 'suppkey_count', 'suppkey_max']]

    '''
    val line1 = plineitem.groupBy($"l_orderkey")
      .agg(countDistinct($"l_suppkey").as("suppkey_count"), max($"l_suppkey").as("suppkey_max"))
      .select($"l_orderkey".as("key"), $"suppkey_count", $"suppkey_max")

    val line2 = flineitem.groupBy($"l_orderkey")
      .agg(countDistinct($"l_suppkey").as("suppkey_count"), max($"l_suppkey").as("suppkey_max"))
      .select($"l_orderkey".as("key"), $"suppkey_count", $"suppkey_max")

    val forder = order.select($"o_orderkey", $"o_orderstatus")
      .filter($"o_orderstatus" === "F")

    nation.filter($"n_name" === "SAUDI ARABIA")
      .join(fsupplier, $"n_nationkey" === fsupplier("s_nationkey"))
      .join(flineitem, $"s_suppkey" === flineitem("l_suppkey"))
      .join(forder, $"l_orderkey" === forder("o_orderkey"))
      ##.join(line1, $"l_orderkey" === line1("key"))
      .filter($"suppkey_count" > 1 || ($"suppkey_count" == 1 && $"l_suppkey" == $"max_suppkey"))
      .select($"s_name", $"l_orderkey", $"l_suppkey")
      .join(line2, $"l_orderkey" === line2("key"), "left_outer")
      ##.select($"s_name", $"l_orderkey", $"l_suppkey", $"suppkey_count", $"suppkey_max")
      .filter($"suppkey_count" === 1 && $"l_suppkey" === $"suppkey_max")
      .groupBy($"s_name")
      .agg(count($"l_suppkey").as("numwait"))
      .sort($"numwait".desc, $"s_name")
      .limit(100)

    '''
    return line1



def query22(tables):
    customer = tables['CUSTOMER']
    orders = tables['ORDERS']

    customer['cntrycode'] = customer['c_phone']
    phone = ['13', '31', '23', '29', '30', '18', '17']
    fcustomer = customer[customer['cntrycode'].isin(phone)][['c_acctbal', 'c_custkey', 'cntrycode']]

    fcustomer['avg_acctbal'] = fcustomer[fcustomer['c_acctbal'] > 0.0]['c_acctbal'].mean()

    cusOrder = orders.groupby('o_custkey').agg({'o_orderkey': 'count'}).\
            merge(fcustomer, left_on='o_custkey', right_on='c_custkey', how='right')

    #custOrder = custOrder # this should implemeted .filter($"o_custkey".isNull)

    #custAvg = custOrder.merge(avg_customer, left_on='o_custkey', right_on='c_custkey')
    #result = custAvg[custAvg['c_acctbal'] > 'avg_acctbal'].\
    #        groupby('cntrycode').\
    #        agg({'c_acctbal': 'count', 'c_acctbal': 'sum'}).\
    #        reset_index(drop=False).set_index('cntrycode')
    result = cusOrder
    

    #val sub2 = udf { (x: String) => x.substring(0, 2) }
    #val phone = udf { (x: String) => x.matches("13|31|23|29|30|18|17") }
    #val isNull = udf { (x: Any) => println(x); true }

    #val fcustomer = customer.select($"c_acctbal", $"c_custkey", sub2($"c_phone").as("cntrycode"))
    #  .filter(phone($"cntrycode"))

    #val avg_customer = fcustomer.filter($"c_acctbal" > 0.0)
    #  .agg(avg($"c_acctbal").as("avg_acctbal"))

    #order.groupBy($"o_custkey")
    #  .agg($"o_custkey").select($"o_custkey")
    #  .join(fcustomer, $"o_custkey" === fcustomer("c_custkey"), "right_outer")
    #  //.filter("o_custkey is null")
    #  .filter($"o_custkey".isNull)
    #  .join(avg_customer)
    #  .filter($"c_acctbal" > $"avg_acctbal")
    #  .groupBy($"cntrycode")
    #  .agg(count($"c_acctbal"), sum($"c_acctbal"))
    #  .sort($"cntrycode")

    return result







# Helper for loading by number.
by_number = {
    1: query1,
    2: query2,
    3: query3,
    4: query4,
    5: query5,
    6: query6,
    7: query7,
    8: query8,
    9: query9,
    10: query10,
    11: query11,
    12: query12,
    13: query13,
    14: query14,
    15: query15,
    16: query16,
    17: query17,
    18: query18,
    19: query19,
    20: query20,
    21: query21,
    22: query22
}
