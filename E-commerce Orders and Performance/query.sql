#1 Finding the first and latest order date of each buyer in each shop.
SELECT 
    buyerid
    , shopid
    , min(order_time) as first_order
    , max(order_time) as latest_order
FROM `hendiana-fsda-revou.dataset1.order_tab`
where buyerid is not null
GROUP BY buyerid, shopid
order by buyerid, shopid

#2 Finding buyer that make more than 1 order in 1 month.
with orders as
    (select
        buyerid
        , date_trunc(date(order_time), month) as month_order
        , count(distinct buyerid) as count_order
    FROM `hendiana-fsda-revou.dataset1.order_tab`
    where buyerid is not null
    GROUP BY 1,2
    order by 1,2
    )
select buyerid, month_order, count_order
from orders
where count_order > 1

#3 Finding the first buyer of each shop.
with main as
    (select
        buyerid
        , shopid
        , order_time
        , rank() over(partition by shopid order by order_time desc) as ranks
    FROM `hendiana-fsda-revou.dataset1.order_tab`
    where buyerid is not null
    GROUP BY 1,2,3
    order by 2
    )
select buyerid, shopid, order_time
from main
where ranks = 1

#4 Finding the TOP 10 Buyer by GMV in Country ID & SG.
with main_a as
    (select
        a.buyerid as buyerid
        , a.order_time as order_time
        , a.gmv as gmv
        , b.country as country
        , rank() over(partition by a.buyerid order by a.gmv desc) as ranks
    FROM `hendiana-fsda-revou.dataset1.order_tab` as a
    left join `hendiana-fsda-revou.dataset1.user_tab` as b on a.buyerid = b.buyerid
    where a.buyerid is not null and country in ('ID','SG')
    GROUP BY 1,2,3,4
    order by 3 desc
    ),
main_b as(
    select buyerid, order_time, gmv, country
    , row_number() over(partition by country order by gmv desc) as ranking
    from main_a
    where ranks = 1
    GROUP BY 1,2,3,4
    order by 3 desc
    )
select buyerid, order_time, gmv, country
from main_b
where ranking in (1,2,3,4,5,6,7,8,9,10)
