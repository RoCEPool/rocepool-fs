"""The 11 TPC-H queries of RapidsSE's regression plan (Q1, Q3, Q4, Q6, Q7,
Q8, Q9, Q10, Q11, Q12, Q14, with the plan's parameters), written for
rocepond: a per-partition part that runs on every node, and a merge.

Layout: lineitem and orders are repartitioned by order key, so every
lineitem/orders join is local to a partition; customer, supplier, part,
partsupp, nation and region are broadcast.

What makes each split correct:
  - sums and counts add up over partitions; an average is a sum over a count;
    a ratio of sums is the ratio of the summed numerators and denominators
  - Q3 groups by l_orderkey, and an order lives in one partition: each
    partition's top 10 contain the overall top 10
  - Q10 groups by customer, whose orders are in every partition: partial
    sums per customer are added before the top 20 is taken
  - Q11 touches broadcast tables only: it runs on one node as it is

tools/rocepond_tpch.py checks every answer against a single DuckDB running
the plan's original SQL over the same data.
"""
from __future__ import annotations

SCHEMA = {
    "nation": [("n_nationkey", "INTEGER"), ("n_name", "VARCHAR"), ("n_regionkey", "INTEGER"),
               ("n_comment", "VARCHAR")],
    "region": [("r_regionkey", "INTEGER"), ("r_name", "VARCHAR"), ("r_comment", "VARCHAR")],
    "part": [("p_partkey", "INTEGER"), ("p_name", "VARCHAR"), ("p_mfgr", "VARCHAR"), ("p_brand", "VARCHAR"),
             ("p_type", "VARCHAR"), ("p_size", "INTEGER"), ("p_container", "VARCHAR"),
             ("p_retailprice", "DECIMAL(15,2)"), ("p_comment", "VARCHAR")],
    "supplier": [("s_suppkey", "INTEGER"), ("s_name", "VARCHAR"), ("s_address", "VARCHAR"),
                 ("s_nationkey", "INTEGER"), ("s_phone", "VARCHAR"), ("s_acctbal", "DECIMAL(15,2)"),
                 ("s_comment", "VARCHAR")],
    "partsupp": [("ps_partkey", "INTEGER"), ("ps_suppkey", "INTEGER"), ("ps_availqty", "INTEGER"),
                 ("ps_supplycost", "DECIMAL(15,2)"), ("ps_comment", "VARCHAR")],
    "customer": [("c_custkey", "INTEGER"), ("c_name", "VARCHAR"), ("c_address", "VARCHAR"),
                 ("c_nationkey", "INTEGER"), ("c_phone", "VARCHAR"), ("c_acctbal", "DECIMAL(15,2)"),
                 ("c_mktsegment", "VARCHAR"), ("c_comment", "VARCHAR")],
    "orders": [("o_orderkey", "BIGINT"), ("o_custkey", "INTEGER"), ("o_orderstatus", "VARCHAR"),
               ("o_totalprice", "DECIMAL(15,2)"), ("o_orderdate", "DATE"), ("o_orderpriority", "VARCHAR"),
               ("o_clerk", "VARCHAR"), ("o_shippriority", "INTEGER"), ("o_comment", "VARCHAR")],
    "lineitem": [("l_orderkey", "BIGINT"), ("l_partkey", "INTEGER"), ("l_suppkey", "INTEGER"),
                 ("l_linenumber", "INTEGER"), ("l_quantity", "DECIMAL(15,2)"),
                 ("l_extendedprice", "DECIMAL(15,2)"), ("l_discount", "DECIMAL(15,2)"),
                 ("l_tax", "DECIMAL(15,2)"), ("l_returnflag", "VARCHAR"), ("l_linestatus", "VARCHAR"),
                 ("l_shipdate", "DATE"), ("l_commitdate", "DATE"), ("l_receiptdate", "DATE"),
                 ("l_shipinstruct", "VARCHAR"), ("l_shipmode", "VARCHAR"), ("l_comment", "VARCHAR")],
}
PARTITIONED = {"lineitem": "l_orderkey", "orders": "o_orderkey"}
# nation and region are whole on every node in RapidsSE's data layout: one copy is taken
REPLICATED_SOURCES = ("nation", "region")


class Tables:
    pass


def prepare(sp, files) -> Tables:
    """`files[table][k]`: the .tbl files of `table` that worker k reads."""
    t = Tables()
    for name, cols in SCHEMA.items():
        share = [list(f) for f in files[name]]
        if name in REPLICATED_SOURCES:
            share = [share[0]] + [[] for _ in share[1:]]
        src = sp.read_csv(name, share, cols)
        setattr(t, name, src.repartition(hash_by=PARTITIONED[name]) if name in PARTITIONED else src.broadcast())
    return t


def q1(sp, t):
    part = sp.partial_sql("""
        select l_returnflag, l_linestatus,
               sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price,
               sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
               sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
               sum(l_discount) as sum_disc, count(*) as count_order
        from {0}
        where l_shipdate <= date '1998-12-01' - interval '90' day
        group by l_returnflag, l_linestatus""", t.lineitem)
    return sp.sql("""
        select l_returnflag, l_linestatus,
               sum(sum_qty) as sum_qty, sum(sum_base_price) as sum_base_price,
               sum(sum_disc_price) as sum_disc_price, sum(sum_charge) as sum_charge,
               sum(sum_qty)::DOUBLE / sum(count_order) as avg_qty,
               sum(sum_base_price)::DOUBLE / sum(count_order) as avg_price,
               sum(sum_disc)::DOUBLE / sum(count_order) as avg_disc,
               sum(count_order) as count_order
        from {0} group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus""", part)


def q3(sp, t):
    part = sp.partial_sql("""
        select l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority
        from customer, {0}, {1}
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey
          and o_orderdate < '1995-03-15' and l_shipdate > '1995-03-15'
        group by l_orderkey, o_orderdate, o_shippriority
        order by revenue desc, o_orderdate limit 10""", t.orders, t.lineitem)
    return sp.sql("select * from {0} order by revenue desc, o_orderdate limit 10", part)


def q4(sp, t):
    part = sp.partial_sql("""
        select o_orderpriority, count(*) as order_count
        from {0}
        where o_orderdate >= date '1993-07-01' and o_orderdate < date '1993-07-01' + interval '3' month
          and exists (select * from {1} where l_orderkey = o_orderkey and l_commitdate < l_receiptdate)
        group by o_orderpriority""", t.orders, t.lineitem)
    return sp.sql("select o_orderpriority, sum(order_count) as order_count from {0} "
                  "group by o_orderpriority order by o_orderpriority", part)


def q6(sp, t):
    part = sp.partial_sql("""
        select sum(l_extendedprice*l_discount) as revenue
        from {0}
        where l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year
          and l_discount between 0.06 - 0.01 and 0.06 + 0.01 and l_quantity < 24""", t.lineitem)
    return sp.sql("select sum(revenue) as revenue from {0}", part)


def q7(sp, t):
    part = sp.partial_sql("""
        select supp_nation, cust_nation, l_year, sum(volume) as revenue
        from (select n1.n_name as supp_nation, n2.n_name as cust_nation,
                     extract(year from l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume
              from supplier, {0}, {1}, customer, nation n1, nation n2
              where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey
                and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey
                and ((n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
                  or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE'))
                and l_shipdate >= '1995-01-01' and l_shipdate <= '1996-12-31') as shipping
        group by supp_nation, cust_nation, l_year""", t.lineitem, t.orders)
    return sp.sql("select supp_nation, cust_nation, l_year, sum(revenue) as revenue from {0} "
                  "group by supp_nation, cust_nation, l_year order by supp_nation, cust_nation, l_year", part)


def q8(sp, t):
    part = sp.partial_sql("""
        select o_year, sum(case when nation = 'BRAZIL' then volume else 0 end) as brazil, sum(volume) as total
        from (select extract(year from o_orderdate) as o_year, l_extendedprice * (1-l_discount) as volume,
                     n2.n_name as nation
              from part, supplier, {0}, {1}, customer, nation n1, nation n2, region
              where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey
                and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey
                and r_name = 'AMERICA' and s_nationkey = n2.n_nationkey
                and o_orderdate between date '1995-01-01' and date '1996-12-31'
                and p_type = 'ECONOMY ANODIZED STEEL') as all_nations
        group by o_year""", t.lineitem, t.orders)
    return sp.sql("select o_year, sum(brazil) / sum(total) as mkt_share from {0} group by o_year order by o_year",
                  part)


def q9(sp, t):
    part = sp.partial_sql("""
        select nation, o_year, sum(amount) as sum_profit
        from (select n_name as nation, extract(year from o_orderdate) as o_year,
                     l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
              from part, supplier, {0}, partsupp, {1}, nation
              where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey
                and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey
                and p_name like '%green%') as profit
        group by nation, o_year""", t.lineitem, t.orders)
    return sp.sql("select nation, o_year, sum(sum_profit) as sum_profit from {0} "
                  "group by nation, o_year order by nation, o_year desc", part)


def q10(sp, t):
    part = sp.partial_sql("""
        select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue,
               c_acctbal, n_name, c_address, c_phone, c_comment
        from customer, {0}, {1}, nation
        where c_custkey = o_custkey and l_orderkey = o_orderkey
          and o_orderdate >= date '1993-10-01' and o_orderdate < date '1993-10-01' + interval '3' month
          and l_returnflag = 'R' and c_nationkey = n_nationkey
        group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment""", t.orders, t.lineitem)
    return sp.sql("""
        select c_custkey, c_name, sum(revenue) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment
        from {0}
        group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
        order by revenue desc limit 20""", part)


def q11(sp, t):
    return sp.sql("""
        select ps_partkey, sum(ps_supplycost * ps_availqty) as value
        from {0}, {1}, {2}
        where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'
        group by ps_partkey
        having sum(ps_supplycost * ps_availqty) > (
            select sum(ps_supplycost * ps_availqty) * 0.0001
            from {0}, {1}, {2}
            where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY')
        order by value desc""", t.partsupp, t.supplier, t.nation)


def q12(sp, t):
    part = sp.partial_sql("""
        select l_shipmode,
               sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end)
                   as high_line_count,
               sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end)
                   as low_line_count
        from {0}, {1}
        where o_orderkey = l_orderkey and l_shipmode in ('MAIL', 'SHIP') and l_commitdate < l_receiptdate
          and l_shipdate < l_commitdate and l_receiptdate >= date '1994-01-01'
          and l_receiptdate < date '1994-01-01' + interval '1' year
        group by l_shipmode""", t.orders, t.lineitem)
    return sp.sql("select l_shipmode, sum(high_line_count) as high_line_count, "
                  "sum(low_line_count) as low_line_count from {0} group by l_shipmode order by l_shipmode", part)


def q14(sp, t):
    part = sp.partial_sql("""
        select sum(case when p_type like 'PROMO%' then l_extendedprice*(1-l_discount) else 0 end) as promo,
               sum(l_extendedprice * (1 - l_discount)) as total
        from {0}, part
        where l_partkey = p_partkey and l_shipdate >= date '1995-09-01'
          and l_shipdate < date '1995-09-01' + interval '1' month""", t.lineitem)
    return sp.sql("select 100.00 * sum(promo) / sum(total) as promo_revenue from {0}", part)


QUERIES = {"q1": q1, "q3": q3, "q4": q4, "q6": q6, "q7": q7, "q8": q8, "q9": q9, "q10": q10, "q11": q11,
           "q12": q12, "q14": q14}

# the plan's own text (bamboo/perftest/configs/tests/tpch-sf1-2D2F.jmx), for the single-node reference
REFERENCE_SQL = {
    "q1": """select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order
        from lineitem where l_shipdate <= date '1998-12-01' - interval '90' day
        group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus""",
    "q3": """select l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority
        from customer, orders, lineitem
        where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey
          and o_orderdate < '1995-03-15' and l_shipdate > '1995-03-15'
        group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10""",
    "q4": """select o_orderpriority, count(*) as order_count from orders
        where o_orderdate >= date '1993-07-01' and o_orderdate < date '1993-07-01' + interval '3' month
          and exists (select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate)
        group by o_orderpriority order by o_orderpriority""",
    "q6": """select sum(l_extendedprice*l_discount) as revenue from lineitem
        where l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year
          and l_discount between 0.06 - 0.01 and 0.06 + 0.01 and l_quantity < 24""",
    "q7": """select supp_nation, cust_nation, l_year, sum(volume) as revenue
        from (select n1.n_name as supp_nation, n2.n_name as cust_nation, extract(year from l_shipdate) as l_year,
                     l_extendedprice * (1 - l_discount) as volume
              from supplier, lineitem, orders, customer, nation n1, nation n2
              where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey
                and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey
                and ((n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
                  or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE'))
                and l_shipdate >= '1995-01-01' and l_shipdate <= '1996-12-31') as shipping
        group by supp_nation, cust_nation, l_year order by supp_nation, cust_nation, l_year""",
    "q8": """select o_year, sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share
        from (select extract(year from o_orderdate) as o_year, l_extendedprice * (1-l_discount) as volume,
                     n2.n_name as nation
              from part, supplier, lineitem, orders, customer, nation n1, nation n2, region
              where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey
                and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey
                and r_name = 'AMERICA' and s_nationkey = n2.n_nationkey
                and o_orderdate between date '1995-01-01' and date '1996-12-31'
                and p_type = 'ECONOMY ANODIZED STEEL') as all_nations
        group by o_year order by o_year""",
    "q9": """select nation, o_year, sum(amount) as sum_profit
        from (select n_name as nation, extract(year from o_orderdate) as o_year,
                     l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
              from part, supplier, lineitem, partsupp, orders, nation
              where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey
                and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey
                and p_name like '%green%') as profit
        group by nation, o_year order by nation, o_year desc""",
    "q10": """select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, n_name,
        c_address, c_phone, c_comment
        from customer, orders, lineitem, nation
        where c_custkey = o_custkey and l_orderkey = o_orderkey
          and o_orderdate >= date '1993-10-01' and o_orderdate < date '1993-10-01' + interval '3' month
          and l_returnflag = 'R' and c_nationkey = n_nationkey
        group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
        order by revenue desc limit 20""",
    "q11": """select ps_partkey, sum(ps_supplycost * ps_availqty) as value
        from partsupp, supplier, nation
        where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'
        group by ps_partkey
        having sum(ps_supplycost * ps_availqty) > (
            select sum(ps_supplycost * ps_availqty) * 0.0001 from partsupp, supplier, nation
            where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY')
        order by value desc""",
    "q12": """select l_shipmode,
        sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count,
        sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count
        from orders, lineitem
        where o_orderkey = l_orderkey and l_shipmode in ('MAIL', 'SHIP') and l_commitdate < l_receiptdate
          and l_shipdate < l_commitdate and l_receiptdate >= date '1994-01-01'
          and l_receiptdate < date '1994-01-01' + interval '1' year
        group by l_shipmode order by l_shipmode""",
    "q14": """select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice*(1-l_discount) else 0 end)
        / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
        from lineitem, part
        where l_partkey = p_partkey and l_shipdate >= date '1995-09-01'
          and l_shipdate < date '1995-09-01' + interval '1' month""",
}
