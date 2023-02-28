QUERY_DICT= {
        1 : ["lineitem"],
        2 : ["part", "supplier", "partsupp", "nation" , "region"],
        3 : ["customer", "lineitem", "orders"],
        4 : ["orders", "lineitem"],
        5 : ["customer", "orders", "lineitem", "supplier", "nation", "region"],
        6 : ["lineitem"],
        7 : ["supplier", "lineitem", "orders", "customer", "nation"],
        8 : ["part", "supplier", "lineitem", "orders", "customer", "nation", "region"],
        9 : ["part", "supplier", "lineitem", "partsupp", "orders", "nation"],
        10 : ["customer", "orders", "lineitem", "nation"],
        11 : ["partsupp", "supplier", "nation"],
        12 : ["orders", "lineitem"],
        13 : ["customer", "orders"],
        14 : ["lineitem", "part"],
        15 : ["lineitem", "supplier"],
        16 : ["partsupp", "part", "supplier"],
        17 : ["lineitem", "part"],
        18 : ["customer", "orders","lineitem"],
        19 : ["lineitem", "part"],
        20 : ["supplier", "nation", "partsupp", "part", "lineitem"],
        21 : ["supplier", "lineitem", "orders", "nation"],
        22: ["customer", "orders"]

}

def QUERY_1(lineitem):
        query= f'''select
                l_returnflag,
                l_linestatus,
                sum(l_quantity) as sum_qty,
                sum(l_extendedprice) as sum_base_price,
                sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
                sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
                avg(l_quantity) as avg_qty,
                avg(l_extendedprice) as avg_price,
                avg(l_discount) as avg_disc,
                count(*) as count_order
        from
                {lineitem}
        where
                l_shipdate <= date '1998-12-01' - interval '90' day
        group by
                l_returnflag,
                l_linestatus
        order by
                l_returnflag,
                l_linestatus'''

        return query

def QUERY_2(part, supplier, partsupp, nation , region): 
        query = f'''
select
        s_acctbal,
        s_name,
        n_name,
        p_partkey,
        p_mfgr,
        s_address,
        s_phone,
        s_comment
from
        {part},
        {supplier},
        {partsupp},
        {nation},
        {region}
where
        p_partkey = ps_partkey
        and s_suppkey = ps_suppkey
        and p_size = 15
        and p_type like '%BRASS'
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'EUROPE'
        and ps_supplycost = (
                select
                        min(ps_supplycost)
                from
                        {partsupp},
                        {supplier},
                        {nation},
                        {region}
                where
                        p_partkey = ps_partkey
                        and s_suppkey = ps_suppkey
                        and s_nationkey = n_nationkey
                        and n_regionkey = r_regionkey
                        and r_name = 'EUROPE'
        )
order by
        s_acctbal desc,
        n_name,
        s_name,
        p_partkey
limit 100
        '''
        return query

def QUERY_3(customer, lineitem, orders):
        query= f'''
                select
                        l.l_orderkey,
                        sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,
                        o.o_orderdate,
                        o.o_shippriority
                from
                        {customer} c,
                        {orders} o,
                        {lineitem} l
                where
                        c.c_mktsegment = 'BUILDING'
                        and c.c_custkey = o.o_custkey
                        and l.l_orderkey = o.o_orderkey
                        and o.o_orderdate < date '1995-03-15'
                        and l.l_shipdate > date '1995-03-15'
                group by
                        l.l_orderkey,
                        o.o_orderdate,
                        o.o_shippriority
                order by
                        revenue desc,
                        o.o_orderdate
                limit 10

        '''
        return query 

def QUERY_4(orders, lineitem):
        query= f'''
                        select
                        o_orderpriority,
                        count(*) as order_count
                from
                        {orders}
                where
                        o_orderdate >= date '1993-07-01'
                        and o_orderdate < date '1993-10-01'
                        and exists (
                                select
                                        *
                                from
                                        {lineitem}
                                where
                                        l_orderkey = o_orderkey
                                        and l_commitdate < l_receiptdate
                        )
                group by
                        o_orderpriority
                order by
                        o_orderpriority
        '''
        return query       

def QUERY_5(customer, orders, lineitem, supplier, nation, region):
        query= f'''
                select
                        n.n_name,
                        sum(l.l_extendedprice * (1 - l.l_discount)) as revenue
                from
                        {customer} c,
                        {orders} o,
                        {lineitem} l,
                        {supplier} s,
                        {nation} n,
                        {region} r
                where
                        c.c_custkey = o.o_custkey
                        and l.l_orderkey = o.o_orderkey
                        and l.l_suppkey = s.s_suppkey
                        and c.c_nationkey = s.s_nationkey
                        and s.s_nationkey = n.n_nationkey
                        and n.n_regionkey = r.r_regionkey
                        and r.r_name = 'ASIA'
                        and o.o_orderdate >= date '1994-01-01'
                        and o.o_orderdate < date '1995-01-01'
                group by
                        n.n_name
                order by
                        revenue desc
        '''
        return query       


def QUERY_6(lineitem):
        query= f'''select
        sum(l_extendedprice * l_discount) as revenue
        from
                {lineitem}
        where
                l_shipdate >= date '1994-01-01'
                and l_shipdate < date '1995-01-01'
                and l_discount between 0.06 - 0.01 and 0.06 + 0.01
                and l_quantity < 24'''

        return query

def QUERY_7(supplier, lineitem, orders, customer, nation):
        query= f'''select
        supp_nation,
        cust_nation,
        l_year,
        sum(volume) as revenue
from
        (
                select
                        n1.n_name as supp_nation,
                        n2.n_name as cust_nation,
                        extract(year from l.l_shipdate) as l_year,
                        l.l_extendedprice * (1 - l.l_discount) as volume
                from
                        {supplier} s,
                        {lineitem} l,
                        {orders} o,
                        {customer} c,
                        {nation} n1,
                        {nation} n2
                where
                        s.s_suppkey = l.l_suppkey
                        and o.o_orderkey = l.l_orderkey
                        and c.c_custkey = o.o_custkey
                        and s.s_nationkey = n1.n_nationkey
                        and c.c_nationkey = n2.n_nationkey
                        and (
                                (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
                                or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
                        )
                        and l.l_shipdate between date '1995-01-01' and date '1996-12-31'
        ) as shipping
group by
        supp_nation,
        cust_nation,
        l_year
order by
        supp_nation,
        cust_nation,
        l_year
'''

        return query

def QUERY_8(part, supplier, lineitem, orders, customer, nation, region):
        query= f'''select
        o_year,
        sum(case
                when nation = 'BRAZIL' then volume
                else 0
        end) / sum(volume) as mkt_share
from
        (
                select
                        extract(year from o.o_orderdate) as o_year,
                        l.l_extendedprice * (1 - l.l_discount) as volume,
                        n2.n_name as nation
                from
                        {part} p,
                        {supplier} s,
                        {lineitem} l,
                        {orders} o,
                        {customer} c,
                        {nation} n1,
                        {nation} n2,
                        {region} r
                where
                        p.p_partkey = l.l_partkey
                        and s.s_suppkey = l.l_suppkey
                        and l.l_orderkey = o.o_orderkey
                        and o.o_custkey = c.c_custkey
                        and c.c_nationkey = n1.n_nationkey
                        and n1.n_regionkey = r.r_regionkey
                        and r.r_name = 'AMERICA'
                        and s.s_nationkey = n2.n_nationkey
                        and o.o_orderdate between date '1995-01-01' and date '1996-12-31'
                        and p.p_type = 'ECONOMY ANODIZED STEEL'
        ) as all_nations
group by
        o_year
order by
        o_year'''

        return query

def QUERY_9(part, supplier, lineitem, partsupp, orders, nation):
        query= f'''select
        nation,
        o_year,
        sum(amount) as sum_profit
from
        (
                select
                        n.n_name as nation,
                        extract(year from o.o_orderdate) as o_year,
                        l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity as amount
                from
                        {part} p,
                        {supplier} s,
                        {lineitem} l,
                        {partsupp} ps,
                        {orders} o,
                        {nation} n
                where
                        s.s_suppkey = l.l_suppkey
                        and ps.ps_suppkey = l.l_suppkey
                        and ps.ps_partkey = l.l_partkey
                        and p.p_partkey = l.l_partkey
                        and o.o_orderkey = l.l_orderkey
                        and s.s_nationkey = n.n_nationkey
                        and p.p_name like '%green%'
        ) as profit
group by
        nation,
        o_year
order by
        nation,
        o_year desc'''

        return query


def QUERY_10(customer, orders, lineitem, nation):
        query= f'''select
        c.c_custkey,
        c.c_name,
        sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,
        c.c_acctbal,
        n.n_name,
        c.c_address,
        c.c_phone,
        c.c_comment
from
        {customer} c,
        {orders} o,
        {lineitem} l,
        {nation} n
where
        c.c_custkey = o.o_custkey
        and l.l_orderkey = o.o_orderkey
        and o.o_orderdate >= date '1993-10-01'
        and o.o_orderdate < date '1994-01-01'
        and l.l_returnflag = 'R'
        and c.c_nationkey = n.n_nationkey
group by
        c.c_custkey,
        c.c_name,
        c.c_acctbal,
        c.c_phone,
        n.n_name,
        c.c_address,
        c.c_comment
order by
        revenue desc
limit 20'''

        return query

def QUERY_11(partsupp, supplier, nation):
        query= f'''select
        p.ps_partkey,
        sum(p.ps_supplycost * p.ps_availqty) as ps_sum
from
         {partsupp} p,
        {supplier} s,
        {nation} n
where
        p.ps_suppkey = s.s_suppkey
        and s.s_nationkey = n.n_nationkey
        and n.n_name = 'GERMANY'
group by
        p.ps_partkey having
                sum(p.ps_supplycost * p.ps_availqty) > (
                        select
                                sum(p1.ps_supplycost * p1.ps_availqty) * 0.0001
                        from
                                 {partsupp} p1,
                                {supplier} s1,
                                {nation} n1
                        where
                                p1.ps_suppkey = s1.s_suppkey
                                and s1.s_nationkey = n1.n_nationkey
                                and n1.n_name = 'GERMANY'
                )
order by
        ps_sum desc
        '''

        return query



def QUERY_12(orders, lineitem):
        query= f'''select
        l.l_shipmode,
        sum(case
                when o.o_orderpriority = '1-URGENT'
                        or o.o_orderpriority = '2-HIGH'
                        then 1
                else 0
        end) as high_line_count,
        sum(case
                when o.o_orderpriority <> '1-URGENT'
                        and o.o_orderpriority <> '2-HIGH'
                        then 1
                else 0
        end) as low_line_count
from
         {orders} o,
        {lineitem} l
where
        o.o_orderkey = l.l_orderkey
        and l.l_shipmode in ('MAIL', 'SHIP')
        and l.l_commitdate < l.l_receiptdate
        and l.l_shipdate < l.l_commitdate
        and l.l_receiptdate >= date '1994-01-01'
        and l.l_receiptdate < date '1995-01-01'
group by
        l.l_shipmode
order by
        l.l_shipmode'''

        return query

def QUERY_13(customer, orders):
        query= f'''select
        c_count,
        count(*) as custdist
from
        (
                select
                        c.c_custkey,
                        count(o.o_orderkey) c_count
                from
                        {customer} c left outer join {orders} o on
                                c.c_custkey = o.o_custkey
                                and o.o_comment not like '%special%requests%'
                group by
                        c.c_custkey
        ) as c_orders
group by
        c_count
order by
        custdist desc,
        c_count desc
'''

        return query

def QUERY_14(lineitem, part):
        query= f'''select
        100.00 * sum(case
                when p.p_type like 'PROMO%'
                        then l.l_extendedprice * (1 - l.l_discount)
                else 0
        end) / sum(l.l_extendedprice * (1 - l.l_discount)) as promo_revenue
from
        {lineitem} l,
        {part} p
where
        l.l_partkey = p.p_partkey
        and l.l_shipdate >= date '1995-09-01'
        and l.l_shipdate < date '1995-10-01'
        '''

        return query

def QUERY_15(lineitem, supplier):
        query= f'''with revenue as (
	select
		l.l_suppkey as supplier_no,
		sum(l.l_extendedprice * (1 - l.l_discount)) as total_revenue
	from
		{lineitem} l
	where
		l.l_shipdate >= date '1996-01-01'
		and l.l_shipdate < date '1996-04-01'
	group by
		l.l_suppkey)
select
	s.s_suppkey,
	s.s_name,
	s.s_address,
	s.s_phone,
	r.total_revenue
from
	{supplier} s,
	revenue r
where
	s.s_suppkey = r.supplier_no
	and r.total_revenue = (
		select
			max(r1.total_revenue)
		from
			revenue r1
	)
order by
	s.s_suppkey'''

        return query

def QUERY_16(partsupp, part, supplier):
        query= f'''select
        p.p_brand,
        p.p_type,
        p.p_size,
        count(distinct ps.ps_suppkey) as supplier_cnt
from
        {partsupp} ps,
        {part} p
where
        p.p_partkey = ps.ps_partkey
        and p.p_brand <> 'Brand#45'
        and p.p_type not like 'MEDIUM POLISHED%'
        and p.p_size in (49, 14, 23, 45, 19, 3, 36, 9)
        and ps.ps_suppkey not in (
                select
                        s.s_suppkey
                from
                        {supplier} s
                where
                        s.s_comment like '%Customer%Complaints%'
        )
group by
        p.p_brand,
        p.p_type,
        p.p_size
order by
        supplier_cnt desc,
        p.p_brand,
        p.p_type,
        p.p_size'''

        return query

def QUERY_17(lineitem, part):
        query= f'''select
        sum(l.l_extendedprice) / 7.0 as avg_yearly
from
        {lineitem} l,
        {part} p
where
        p.p_partkey = l.l_partkey
        and p.p_brand = 'Brand#23'
        and p.p_container = 'MED BOX'
        and l.l_quantity < (
                select
                        0.2 * avg(l1.l_quantity)
                from
                        {lineitem} l1
                where
                        l1.l_partkey = p.p_partkey
        )
'''

        return query

def QUERY_18(customer, orders,lineitem):
        query= f'''select
        c.c_name,
        c.c_custkey,
        o.o_orderkey,
        o.o_orderdate,
        o.o_totalprice,
        sum(l.l_quantity)
from
        {customer} c,
        {orders} o,
        {lineitem} l
where
        o.o_orderkey in (
                select
                        l1.l_orderkey
                from
                        {lineitem} l1
                group by
                        l1.l_orderkey having
                                sum(l1.l_quantity) > 300
        )
        and c.c_custkey = o.o_custkey
        and o.o_orderkey = l.l_orderkey
group by
        c.c_name,
        c.c_custkey,
        o.o_orderkey,
        o.o_orderdate,
        o.o_totalprice
order by
        o.o_totalprice desc,
        o.o_orderdate
limit 100'''

        return query

def QUERY_19(lineitem, part):
        query= f'''select
        sum(l.l_extendedprice* (1 - l.l_discount)) as revenue
from
        {lineitem} l,
        {part} p
where
        (
                p.p_partkey = l.l_partkey
                and p.p_brand = 'Brand#12'
                and p.p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                and l.l_quantity >= 1 and l.l_quantity <= 1 + 10
                and p.p_size between 1 and 5
                and l.l_shipmode in ('AIR', 'AIR REG')
                and l.l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
                p.p_partkey = l.l_partkey
                and p.p_brand = 'Brand#23'
                and p.p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                and l.l_quantity >= 10 and l.l_quantity <= 10 + 10
                and p.p_size between 1 and 10
                and l.l_shipmode in ('AIR', 'AIR REG')
                and l.l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
                p.p_partkey = l.l_partkey
                and p.p_brand = 'Brand#34'
                and p.p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                and l.l_quantity >= 20 and l.l_quantity <= 20 + 10
                and p.p_size between 1 and 15
                and l.l_shipmode in ('AIR', 'AIR REG')
                and l.l_shipinstruct = 'DELIVER IN PERSON'
        )
'''

        return query

def QUERY_20(supplier, nation, partsupp, part, lineitem):
        query= f'''select
        s.s_name,
        s.s_address
from
        {supplier} s,
        {nation} n
where
        s.s_suppkey in (
                select
                        ps.ps_suppkey
                from
                        {partsupp} ps
                where
                        ps.ps_partkey in (
                                select
                                        p.p_partkey
                                from
                                        {part} p
                                where
                                        p.p_name like 'forest%'
                        )
                        and ps.ps_availqty > (
                                select
                                        0.5 * sum(l.l_quantity)
                                from
                                        {lineitem} l
                                where
                                        l.l_partkey = ps.ps_partkey
                                        and l.l_suppkey = ps.ps_suppkey
                                        and l.l_shipdate >= date '1994-01-01'
                                        and l.l_shipdate < date '1995-01-01'
                        )
        )
        and s.s_nationkey = n.n_nationkey
        and n.n_name = 'CANADA'
order by
        s.s_name'''

        return query

def QUERY_21(supplier, lineitem, orders, nation):
        query= f'''select
        s.s_name,
        count(*) as numwait
from
        {supplier} s,
        {lineitem} l1,
        {orders} o,
        {nation} n
where
        s.s_suppkey = l1.l_suppkey
        and o.o_orderkey = l1.l_orderkey
        and o.o_orderstatus = 'F'
        and l1.l_receiptdate > l1.l_commitdate
        and exists (
                select
                        *
                from
                        {lineitem} l2
                where
                        l2.l_orderkey = l1.l_orderkey
                        and l2.l_suppkey <> l1.l_suppkey
        )
        and not exists (
                select
                        *
                from
                        {lineitem} l3
                where
                        l3.l_orderkey = l1.l_orderkey
                        and l3.l_suppkey <> l1.l_suppkey
                        and l3.l_receiptdate > l3.l_commitdate
        )
        and s.s_nationkey = n.n_nationkey
        and n.n_name = 'SAUDI ARABIA'
group by
        s.s_name
order by
        numwait desc,
        s.s_name
limit 100'''

        return query

def QUERY_22(customer, orders):
        query= f'''select
        cntrycode,
        count(*) as numcust,
        sum(c_acctbal) as totacctbal
from
        (
                select
                        substring(c_phone from 1 for 2) as cntrycode,
                        c_acctbal
                from
                        {customer}
                where
                        substring(c_phone from 1 for 2) in
                                ('13', '31', '23', '29', '30', '18', '17')
                        and c_acctbal > (
                                select
                                        avg(c_acctbal)
                                from
                                        {customer}
                                where
                                        c_acctbal > 0.00
                                        and substring(c_phone from 1 for 2) in
                                                ('13', '31', '23', '29', '30', '18', '17')
                        )
                        and not exists (
                                select
                                        *
                                from
                                        {orders}
                                where
                                        o_custkey = c_custkey
                        )
        ) as custsale
group by
        cntrycode
order by
        cntrycode'''

        return query


def get_queries(customer = "customer", lineitem="lineitem", nation = "nation", orders= "orders", part = "part", partsupp = "partsupp", region = "region", supplier = "supplier"):
        queries = [QUERY_1(lineitem), QUERY_2(part, supplier, partsupp, nation , region), QUERY_3(customer, lineitem, orders), 
        QUERY_4(orders, lineitem), QUERY_5(customer, orders, lineitem, supplier, nation, region), QUERY_6(lineitem), QUERY_7(supplier, lineitem, orders, customer, nation),
        QUERY_8(part, supplier, lineitem, orders, customer, nation, region),
        QUERY_9(part, supplier, lineitem, partsupp, orders, nation),
        QUERY_10(customer, orders, lineitem, nation),
        QUERY_11(partsupp, supplier, nation),  QUERY_12(orders, lineitem), QUERY_13(customer, orders), QUERY_14(lineitem, part),
        QUERY_15(lineitem, supplier), QUERY_16(partsupp, part, supplier), QUERY_17(lineitem, part), QUERY_18(customer, orders,lineitem),
        QUERY_19(lineitem, part), QUERY_20(supplier, nation, partsupp, part, lineitem), QUERY_21(supplier, lineitem, orders, nation),
         QUERY_22(customer, orders)
        ]

        return queries


NUM_QUERIES = 22
QUERIES_INDEX = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22]



