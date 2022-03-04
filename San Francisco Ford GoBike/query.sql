#NUMBER 1: Create a query to get the average amount of duration (in minutes) per month (start date from 2014-2017)
select 
    date_trunc(date(start_date), month) as month_of_trips
    , avg(duration_sec)/60 as avg_minute_duration
from `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
where start_date between '2014-01-01' and '2017-12-31'
group by 1
order by 1 asc

#Number 2: Create a query to get total trips and total number of unique bikes grouped by region name (start date from 2014-2017).
select
    region.name as region_name
    , count(trip.trip_id) as total_trips
    , count(distinct trip.bike_number) as total_bikes
from `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips` trip
join `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info` station on trip.start_station_id = station.station_id
join `bigquery-public-data.san_francisco_bikeshare.bikeshare_regions` region on region.region_id = station.region_id
where trip.start_date between '2014-01-01' and '2017-12-31'
group by 1
order by 1 asc

#Number 3: Find the youngest and oldest age of the members, for each gender. Assume this year is 2022.
select 
    member_gender
    , 2022-max(member_birth_year) as youngest_age
    , 2022-min(member_birth_year) as oldest_age
from `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
where member_gender in ('Male','Female')
group by 1

#Number 4: Get the latest departure trip in each region with detail below (trip_id, duration_sec, start_date, start_station_name, member_gender, region_name). Start date from 2014-2017.
with main as
    (select 
        trip.trip_id as trip_id
        , trip.duration_sec as duration_sec
        , trip.start_date as trip_date
        , trip.start_station_name as station_name
        , trip.member_gender as gender
        , region.name as region_name
        , rank() over(partition by region.name order by trip.start_date desc) as ranks
    from `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips` trip
    join `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info` station on trip.start_station_id = station.station_id
    join `bigquery-public-data.san_francisco_bikeshare.bikeshare_regions` region on region.region_id = station.region_id
    where trip.start_date between '2014-01-01' and '2017-12-31' and trip.member_gender in ('Male', 'Female')
    order by 6 asc  
    )
select 
    trip_id
    , duration_sec
    , date(trip_date) as latest_departure_trip
    , station_name
    , gender
    , region_name
from main
where ranks = 1

#Number 5: Create a query to get Month to Date of total trips in each region, breakdown by date. Timeframe from November 2017 until December 2017.
with data as
(select 
            date(trip.start_date) as trip_date
            , region.name as region_name
            , count(trip.trip_id) as count_trip
        from `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips` trip
        join `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info` station on trip.start_station_id = station.station_id
        join `bigquery-public-data.san_francisco_bikeshare.bikeshare_regions` region on region.region_id = station.region_id
        where trip.start_date between '2017-11-01' and '2017-12-31'
        group by 2,1
        order by 1 asc 
)
select trip_date, region_name, count_trip
    , sum(count_trip) over(order by trip_date rows between unbounded preceding and current row) as cumulative_trips
from data
where region_name = 'Emeryville'
union distinct
select trip_date, region_name, count_trip
    , sum(count_trip) over(order by trip_date rows between unbounded preceding and current row) as cumulative_trips
from data
where region_name = 'Berkeley'
union distinct
select trip_date, region_name, count_trip
    , sum(count_trip) over(order by trip_date rows between unbounded preceding and current row) as cumulative_trips
from data
where region_name = 'Oakland'
union distinct
select trip_date, region_name, count_trip
    , sum(count_trip) over(order by trip_date rows between unbounded preceding and current row) as cumulative_trips
from data
where region_name = 'San Francisco'
union distinct
select trip_date, region_name, count_trip
    , sum(count_trip) over(order by trip_date rows between unbounded preceding and current row) as cumulative_trips
from data
where region_name = 'San Jose'

#Number 6: Finding monthly growth of trips in percentage, ordered by time descendingly. Only trips from the region that has the highest total number of trips (in 2014-2017).
with trips as
(select
    date_trunc(date(trip.start_date), month) as month_trip
    , region.name as region_name
    , count(trip.trip_id) as count_trip
    -- , as growth
from `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips` trip
join `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info` station on trip.start_station_id = station.station_id
join `bigquery-public-data.san_francisco_bikeshare.bikeshare_regions` region on region.region_id = station.region_id
where trip.start_date between '2014-01-01' and '2017-12-31' and region.name = 'San Francisco'
group by 2,1
order by 1 desc
),
b as
(select * 
    , lag(count_trip) over(order by month_trip) as leading
from trips
order by month_trip desc
)
select month_trip, region_name, count_trip
    , (count_trip - leading ) / leading * 100 as growth_percentage
from b

#Question 7
with cohort_items as (
  select
    author,
    MIN(date(date_trunc(time_ts,MONTH))) as cohort_month,
  from `bigquery-public-data.hacker_news.stories`
  GROUP BY 1
),
user_activities as (
  select
    act.author as author,
    DATE_DIFF(
      date(date_trunc(time_ts,MONTH)),
      cohort.cohort_month,
      MONTH
    ) as month_number,
  from `bigquery-public-data.hacker_news.stories` act
  left join cohort_items cohort ON act.author = cohort.author
  where extract(year from cohort.cohort_month) in (2014)
  group by 1, 2
),
cohort_size as (
    SELECT  cohort_month,
    count(1) as num_users
    FROM cohort_items
    GROUP BY 1
    ORDER BY 1
),
retention_table as (
  select
    C.cohort_month,
    A.month_number,
    count(1) as num_users
  from user_activities A
  left join cohort_items C ON A.author = C.author
  group by 1, 2
)
-- our final value: (cohort_month, size, month_number, percentage)
select
  B.cohort_month,
  S.num_users as cohort_size,
  B.month_number,
  B.num_users as total_users,
  cast(B.num_users as decimal)/ S.num_users as percentage
from retention_table B
left join cohort_size S ON B.cohort_month = S.cohort_month
where B.cohort_month IS NOT NULL
order by 1, 3 
