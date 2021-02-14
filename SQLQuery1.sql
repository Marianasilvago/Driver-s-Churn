create table dbo.combine_data as ( 
SELECT  
A.driver_id,
A.driver_onboard_date,
B.ride_distance,
B.ride_duration,
B.ride_id,
B.ride_prime_time,
C.[event] as event_,
C.[timestamp] as timestamp_,
--(((ride_distance *  0.00062137) * 1.15) + ((ride_duration  / 60) * 0.22 ) + (ride_prime_time / 60 ) + 3.75) as ride_fare,
CASE
	WHEN (((ride_distance *  0.00062137) * 1.15) + ((ride_duration  / 60) * 0.22 ) + (ride_prime_time / 60 ) + 3.75) < 5 THEN  '5'
	WHEN (((ride_distance *  0.00062137) * 1.15) + ((ride_duration  / 60) * 0.22 ) + (ride_prime_time / 60 ) + 3.75) > 400 THEN  '400'
	ELSE (((ride_distance  * 0.00062137) * 1.15) + ((ride_duration  / 60) * 0.22 ) + (ride_prime_time / 60 ) + 3.75) 
end as ride_fare

INTO dbo.combine_data

from
[dbo].[driver_ids] A
inner join
[dbo].[ride_ids] B
on 
A.driver_id	= B.driver_id
inner join
[dbo].[ride_timestamps] C
on B.ride_id = C.ride_id
);


select top 100 * from dbo.combine_data

SELECT
A.driver_id
,month(A.timestamp_) as _month
--,sum(A.ride_fare)
--,count(distinct A.ride_id) 
,(sum(A.ride_fare)/count(distinct A.ride_id)) as Life_time
from (select distinct ride_id, driver_id, ride_fare, timestamp_ from dbo.combine_data) A
GROUP BY driver_id;





select 
A.driver_id
,A.month_
,(sum(A.ride_fare)/count(distinct A.ride_id)) as Life_time
from 
(
select A.driver_id, A.ride_distance, A.ride_duration, A.ride_fare, A.ride_id, month(A.timestamp_) as month_
from (select * from dbo.combine_data where event_ = 'requested_at') A
inner join (select * from dbo.combine_data where event_ = 'accepted_at') B
on A.ride_id = B.ride_id
inner join (select * from dbo.combine_data where event_ = 'arrived_at') C
on B.ride_id = C.ride_id
inner join (select * from dbo.combine_data where event_ = 'picked_up_at') D
on C.ride_id = D.ride_id
inner join (select * from dbo.combine_data where event_ = 'dropped_off_at') E
on D.ride_id = E.ride_id
where A.timestamp_ < B.timestamp_
and B.timestamp_ < C.timestamp_
and C.timestamp_ < D.timestamp_
and D.timestamp_ < E.timestamp_
)A
GROUP BY driver_id, month_
order by driver_id,month_;






select 
XX.driver_id,
AVG(XX.Life_time) as AVG_LIFE_TIME
from
(
select
A.driver_id
,A.month_
,(sum(A.ride_fare)/count(distinct A.ride_id)) as Life_time
from 
(
select A.driver_id, A.ride_distance, A.ride_duration, A.ride_fare, A.ride_id, month(A.timestamp_) as month_
from (select * from dbo.combine_data where event_ = 'requested_at') A
inner join (select * from dbo.combine_data where event_ = 'accepted_at') B
on A.ride_id = B.ride_id
inner join (select * from dbo.combine_data where event_ = 'arrived_at') C
on B.ride_id = C.ride_id
inner join (select * from dbo.combine_data where event_ = 'picked_up_at') D
on C.ride_id = D.ride_id
inner join (select * from dbo.combine_data where event_ = 'dropped_off_at') E
on D.ride_id = E.ride_id
where A.timestamp_ < B.timestamp_
and B.timestamp_ < C.timestamp_
and C.timestamp_ < D.timestamp_
and D.timestamp_ < E.timestamp_
)A
GROUP BY driver_id, month_
)XX
group by driver_id;

--where driver_id = '3caf85a7d6b8f5615fbc21e8310af584'
order by A.month_ 

from
(select timestamp_
from dbo.combine_data)A;


order by Life_time desc;




select * from 
dbo.combine_data
where driver_id= '7c27405cefee2fad79a81a819ca9dbe1'

group by 

select [driver_onboard_date] from dbo.combine_data;

select CONVERT(DATETIME, [driver_onboard_date],  105)
from dbo.combine_data





