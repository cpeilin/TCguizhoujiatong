--近7天中位数，方差
drop table if exists cpl_temp7D;
create table if not exists cpl_temp7D as 
select link_id,hour,minute,median(travel_time) as mea_7d,stddev(travel_time) as std_7d
from lzh_all_training_set where (month=6)and(year=2017)and((hour=8)or(hour=15)or(hour=18))and( day>22 and day<31)
group by link_id,hour,minute;--,sum((travel_time-avg(travel_time))/stddev( travel_time)) as kurt_time 

--求众数
create table cpl_tempmode7D as 
select link_id,hour,minute,travel_time,count(travel_time) as count_time 
from lzh_all_training_set where (month=6)and(year=2017)and((hour=8)or(hour=15)or(hour=18))and( day>22 and day<31)
group by link_id,hour,minute,travel_time;


drop table if exists cpl_mode7D;
create table if not exists cpl_mode7D as 
select link_id,hour,minute,avg(travel_time) as mode_travel_time7D from(
select a.link_id,a.hour,a.minute,a.travel_time from cpl_tempmode7D a join 
(select link_id,hour,minute,max(count_time) as count_time from cpl_tempmode7D group by link_id,hour,minute)b 
on (a.link_id =b.link_id)and(a.minute =b.minute)and(a.count_time =b.count_time)and(a.hour =b.hour))c
group by link_id,hour,minute;

drop table cpl_tempmode7D;

--select count(*) from cpl_mode7D;
--select count(*) from cpl_temp7D;
--生成7天的总表包括众数、中位数
drop table if exists cpl_hengxiang7dtest;
create table if not exists cpl_hengxiang7dtest as 
select a.*,b.mode_travel_time7d  from cpl_temp7D a join cpl_mode7D b
on (a.link_id =b.link_id)and(a.minute =b.minute)and(a.hour =b.hour);
--select count(*) from cpl_hengxiang7d;

drop table cpl_temp7D;
drop table cpl_mode7D;

--近15天
drop table if exists cpl_temp15D;
create table if not exists cpl_temp15D as 
select link_id,hour,minute,median(travel_time) as mea_15d,stddev(travel_time) as std_15d,avg(travel_time) as av_15d
from lzh_all_training_set where (month=6)and(year=2017)and((hour=8)or(hour=15)or(hour=18))and( day>15 and day<31)
group by link_id,hour,minute;--,sum((travel_time-avg(travel_time))/stddev( travel_time)) as kurt_time 

--求众数
create table cpl_tempmode15D as 
select link_id,hour,minute,travel_time,count(travel_time) as count_time 
from lzh_all_training_set where (month=6)and(year=2017)and((hour=8)or(hour=15)or(hour=18))and( day>15 and day<31)
group by link_id,hour,minute,travel_time;

drop table if exists cpl_mode15D;
create table if not exists cpl_mode15D as 
select link_id,hour,minute,avg(travel_time) as mode_travel_time15D from(
select a.link_id,a.hour,a.minute,a.travel_time from cpl_tempmode15D a join 
(select link_id,hour,minute,max(count_time) as count_time from cpl_tempmode15D group by link_id,hour,minute)b 
on (a.link_id =b.link_id)and(a.minute =b.minute)and(a.count_time =b.count_time)and(a.hour =b.hour))c
group by link_id,hour,minute;

drop table cpl_tempmode15D;
--生成15天的总表包括众数、中位数
drop table if exists cpl_hengxiang15dtest;
create table if not exists cpl_hengxiang15dtest as 
select a.*,b.mode_travel_time15d  from cpl_temp15D a join cpl_mode15D b
on (a.link_id =b.link_id)and(a.minute =b.minute)and(a.hour =b.hour);
--select count(*) from cpl_hengxiang7d;

drop table cpl_temp15D;
drop table cpl_mode15D;

--近30天
drop table if exists cpl_temp30D;
create table if not exists cpl_temp30D as 
select link_id,hour,minute,median(travel_time) as mea_30d,stddev(travel_time) as std_30d,avg(travel_time) as av_30d
from lzh_all_training_set where (month=6)and(year=2017)and((hour=8)or(hour=15)or(hour=18))and( day>0 and day<31)
group by link_id,hour,minute;--,sum((travel_time-avg(travel_time))/stddev( travel_time)) as kurt_time 

--求众数
create table cpl_tempmode30D as 
select link_id,hour,minute,travel_time,count(travel_time) as count_time 
from lzh_all_training_set where (month=6)and(year=2017)and((hour=8)or(hour=15)or(hour=18))and( day>0 and day<31)
group by link_id,hour,minute,travel_time;


drop table if exists cpl_mode30D;
create table if not exists cpl_mode30D as 
select link_id,hour,minute,avg(travel_time) as mode_travel_time30D from(
select a.link_id,a.hour,a.minute,a.travel_time from cpl_tempmode30D a join 
(select link_id,hour,minute,max(count_time) as count_time from cpl_tempmode30D group by link_id,hour,minute)b 
on (a.link_id =b.link_id)and(a.minute =b.minute)and(a.count_time =b.count_time)and(a.hour =b.hour))c
group by link_id,hour,minute;

drop table cpl_tempmode30D;
--生成30天的总表包括众数、中位数
drop table if exists cpl_hengxiang30dtest;
create table if not exists cpl_hengxiang30dtest as 
select a.*,b.mode_travel_time30d  from cpl_temp30D a join cpl_mode30D b
on (a.link_id =b.link_id)and(a.minute =b.minute)and(a.hour =b.hour);

drop table cpl_temp30D;
drop table cpl_mode30D;

--近60天
drop table if exists cpl_temp60D;
create table if not exists cpl_temp60D as 
select link_id,hour,minute,median(travel_time) as mea_60d,stddev(travel_time) as std_60d,avg(travel_time) as av_60d
from lzh_all_training_set where (month=5)or(month=6)and(year=2017)and((hour=8)or(hour=15)or(hour=18))and(day>0 and day<31)
group by link_id,hour,minute;--,sum((travel_time-avg(travel_time))/stddev( travel_time)) as kurt_time 

--求众数
create table cpl_tempmode60D as 
select link_id,hour,minute,travel_time,count(travel_time) as count_time 
from lzh_all_training_set where (month=5)or(month=6)and(year=2017)and((hour=8)or(hour=15)or(hour=18))and( day>0 and day<31)
group by link_id,hour,minute,travel_time;


drop table if exists cpl_mode60D;
create table if not exists cpl_mode60D as 
select link_id,hour,minute,avg(travel_time) as mode_travel_time60D from(
select a.link_id,a.hour,a.minute,a.travel_time from cpl_tempmode60D a join 
(select link_id,hour,minute,max(count_time) as count_time from cpl_tempmode60D group by link_id,hour,minute)b 
on (a.link_id =b.link_id)and(a.minute =b.minute)and(a.count_time =b.count_time)and(a.hour =b.hour))c
group by link_id,hour,minute;

drop table cpl_tempmode60D;
--生成60天的总表包括众数、中位数
drop table if exists cpl_hengxiang60dtest;
create table if not exists cpl_hengxiang60dtest as 
select a.*,b.mode_travel_time60d  from cpl_temp60D a join cpl_mode60D b
on (a.link_id =b.link_id)and(a.minute =b.minute)and(a.hour =b.hour);
--select count(*) from cpl_hengxiang7d;

drop table cpl_temp60D;
drop table cpl_mode60D;
-----------
--生成6月份的线上训练特征表
drop table if exists cpl_6online_test;
create table if not exists cpl_6online_test as 
select a.link_id,a.year,a.month,a.day,a.hour,a.minute,a.travel_time,e.mea_7d ,e.std_7d ,e.mode_travel_time7d
,d.mea_15d ,d.std_15d ,d.av_15d ,d.mode_travel_time15d ,d.av_15d - d.std_15d as sub15
,c.mea_30d ,c.std_30d ,c.av_30d ,c.mode_travel_time30d,c.av_30d - c.std_30d as sub30
,b.mea_60d ,b.std_60d ,b.av_60d ,b.mode_travel_time60d,b.av_60d - b.std_60d as sub60
from (select * from lzh_all_training_set_addtest where (month=7)and(year=2017)and((hour=8)or(hour=15)or(hour=18))) a 
left outer join cpl_hengxiang60dtest b
on (a.link_id =b.link_id)and(a.minute =b.minute)and(a.hour =b.hour)
left outer join cpl_hengxiang30dtest c
on (a.link_id =c.link_id)and(a.minute =c.minute)and(a.hour =c.hour)
left outer join cpl_hengxiang15dtest d
on (a.link_id =d.link_id)and(a.minute =d.minute)and(a.hour =d.hour)
left outer join cpl_hengxiang7dtest e
on (a.link_id =e.link_id)and(a.minute =e.minute)and(a.hour =e.hour)