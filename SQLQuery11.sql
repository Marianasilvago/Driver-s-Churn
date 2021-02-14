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
INTO dbo.new_data
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


-- Question 2

select 
avg(A.count_) as avg_projected 
from 
(select driver_id, count(month_) as count_ from 
dbo.new_data
group by driver_id
)A


select 
count(A.driver_id) as count_driver
from 
(select driver_id, count(month_) as count_ from 
dbo.new_data
group by driver_id
)A
where A.count_ = 4

-- 1 49
-- 2 316
-- 3 409
-- 4 63


-- Question 3
select 
XX.driver_id,
XX.week_number
from
(
select A.driver_id, A.ride_distance, A.ride_duration, A.ride_fare, A.ride_id, month(A.timestamp_) as month_, day(A.timestamp_) as days_, DatePart(week, A.timestamp_) as week_number
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
--order by driver_id, month_,days_
)XX
group by driver_id, week_number
order by driver_id, week_number desc
)YY

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





select 
* from [dbo].[combine_data]
where 
driver_id in 
('011e5c5dfc5c2c92501b8b24d47509bc'
,'02e440f6c209206375833cef02e0cbae'
,'0938ed763cb3129ae63607aaf69daff5'
,'0f057c0c73054f569a59a0880b91cbb0'
,'1127af601413f32c08e2a89447e5f3b9'
,'14183e69946d782c92eb53b3c6eeb86f'
,'1805f90c5220ac830835df3309a76e78'
,'1a2af09b492ba2519c5b4aec478642a7'
,'23017e4c03d224c89b5e3a0550e1bd9f'
,'2d42e6d775b4399a56b71c2f3d6905ab'
,'2f29fdfd281552870b7f07152889e0fb'
,'3777e15bb2eb838394a372e119d96462'
,'37ac4f9379c836b08c9a6e2315fd66af'
,'3f82d353964869022a995e87b480a901'
,'548a85a2fbb4986fd7a632566e8cb9c3'
,'706466935b9e1d04e4e116be7ce90ea9'
,'7b625f643d0775f0ac4898e33235377b'
,'7d8627729a14175b1424153ba46d7acf'
,'b7bd34f3d587ea950dabfd4ac7781f2f'
,'bf0a8a6ddc179a9466082343c5862904'
,'da3325f424c71942ca8a3401615a0e6c'
,'dfe29c1db80f470f4fa8046bc7e5d8a0'
,'e8d73d55baeb522ef8843cb54cd08df0'
,'f86eb77e1cefe28e9f0e9d3775fae261'
,'fed19d671569afe8a2f9fa0953dd25ca'
,'007f0389f9c7b03ef97098422f902e62'
,'579967d1572ee95c66ffa1ca4077b61a'
,'6455eac21c029b8d93ff09c2dffcea35'
,'7f4350f4a358ac264ccf3b10c4966afc'
,'f1b4411717c78f67380366c2a16a4d1e'
)
