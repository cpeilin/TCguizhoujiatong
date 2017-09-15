--特征工程
--上下游特征
drop table if exists cpl_inout4min;
create table if not exists cpl_inout4min as 
select a.*,b.day,b.month,b.hour,b.avio_travel_time,b.medio_travel_time, b.avio_travel_time / a.in_top_length as speed4in
from lzh_in_top a left outer join
(select c.link_id,c.day,c.month,c.hour,avg(c.travel_time) as avio_travel_time,median(c.travel_time) as medio_travel_time from cpl_clean_alltrainset c
where ((c.hour=7 or c.hour=14 or c.hour=17)and(c.minute>=56)and(c.year=2017))
group by c.link_id,c.day,c.month,c.hour)b
on ((a.in_top =b.link_id));

drop table if exists cpl_inout4mingroup;
create table if not exists cpl_inout4mingroup as 
select a.link_id ,a.day ,a.month, a.hour, avg(a.avio_travel_time)as in4min,avg(a.medio_travel_time)as in4min2,avg(a.speed4in)as in4minspeed from cpl_inout4min a 
group by a.link_id ,a.day ,a.month, a.hour; 

drop table if exists cpl_inout6min;
create table if not exists cpl_inout6min as 
select a.*,b.day,b.month,b.hour,b.avio_travel_time,b.medio_travel_time, b.avio_travel_time / a.in_top_length as speed6in
from lzh_in_top a left outer join
(select c.link_id,c.day,c.month,c.hour,avg(c.travel_time) as avio_travel_time,median(c.travel_time) as medio_travel_time from cpl_clean_alltrainset c
where ((c.hour=7 or c.hour=14 or c.hour=17)and(c.minute>=52)and(c.year=2017))
group by c.link_id,c.day,c.month,c.hour)b
on ((a.in_top =b.link_id));

drop table if exists cpl_inout6mingroup;
create table if not exists cpl_inout6mingroup as 
select a.link_id ,a.day ,a.month, a.hour, avg(a.avio_travel_time)as in6min,avg(a.medio_travel_time)as in6min2,avg(a.speed6in)as in6minspeed from cpl_inout6min a 
group by a.link_id ,a.day ,a.month, a.hour; 

--------------------------
drop table if exists cpl_out4min;
create table if not exists cpl_out4min as 
select a.*,b.day,b.month,b.hour,b.avio_travel_time,b.medio_travel_time, b.avio_travel_time / a.out_top_length as speed4in
from lzh_out_top a left outer join
(select c.link_id,c.day,c.month,c.hour,avg(c.travel_time) as avio_travel_time,median(c.travel_time) as medio_travel_time from cpl_clean_alltrainset c
where ((c.hour=7 or c.hour=14 or c.hour=17)and(c.minute>=56)and(c.year=2017))
group by c.link_id,c.day,c.month,c.hour)b
on ((a.out_top =b.link_id));

drop table if exists cpl_out4mingroup;
create table if not exists cpl_out4mingroup as 
select a.link_id ,a.day ,a.month, a.hour, avg(a.avio_travel_time)as out4min,avg(a.medio_travel_time)as out4min2,avg(a.speed4in)as out4minspeed from cpl_out4min a 
group by a.link_id ,a.day ,a.month, a.hour; 

drop table if exists cpl_out6min;
create table if not exists cpl_out6min as 
select a.*,b.day,b.month,b.hour,b.avio_travel_time,b.medio_travel_time, b.avio_travel_time / a.out_top_length as speed6in
from lzh_out_top a left outer join
(select c.link_id,c.day,c.month,c.hour,avg(c.travel_time) as avio_travel_time,median(c.travel_time) as medio_travel_time from cpl_clean_alltrainset c
where ((c.hour=7 or c.hour=14 or c.hour=17)and(c.minute>=52)and(c.year=2017))
group by c.link_id,c.day,c.month,c.hour)b
on ((a.out_top =b.link_id));

truncate table gy_cmp_testing_seg1; 

drop table if exists cpl_out6mingroup;
create table if not exists cpl_out6mingroup as 
select a.link_id ,a.day ,a.month, a.hour, avg(a.avio_travel_time)as out6min,avg(a.medio_travel_time)as out6min2,avg(a.speed6in)as out6minspeed from cpl_out6min a 
group by a.link_id ,a.day ,a.month, a.hour; 

--产生加入上下流的样本				  
drop table if exists cpl_featuretest3addrank2inout;
create table if not exists cpl_featuretest3addrank2inout as 
select a.*,b.out6min ,b.out6min2,b.out6minspeed ,c.out4min ,c.out4min2,c.out4minspeed,d.in6min ,d.in6min2 ,d.in6minspeed ,e.in4min ,e.in4min2,e.in4minspeed 
from cpl_featuretest3addrank2 a 
left outer join (select *,hour+1 as houradd1 from cpl_out6mingroup) b
on (a.link_id =b.link_id and a.day=b.day and a.month =b.month and a.hour =b.houradd1)
left outer join (select *,hour+1 as houradd1 from cpl_out4mingroup) c
on (a.link_id =c.link_id and a.day=c.day and a.month =c.month and a.hour =c.houradd1) 
left outer join (select *,hour+1 as houradd1 from cpl_inout6mingroup) d
on (a.link_id =d.link_id and a.day=d.day and a.month =d.month and a.hour =d.houradd1)
left outer join (select *,hour+1 as houradd1 from cpl_inout4mingroup) e
on (a.link_id =e.link_id and a.day=e.day and a.month =e.month and a.hour =e.houradd1); 


--4,5,6,7月前1/2小时的时间衰减的累积和15分钟一个间隔(填充后的)
---------------------------------------------------------------------------
--产生各月每一天前一小时内的车流量排序
--drop table if exists cpl_rank2;
--create table if not exists cpl_rank2 as 
--select a.*, b.rank2 from
--((select *from lzh_all_training_set_addtest where (year=2017 and (month=4 or month=5 or month=6 or month=7) and (hour=8 or hour=15 or hour=18))) a
--left outer join 
--(select *,hour+1 as houradd1, row_number() over(partition by year,month,day,hour ORDER BY travel_time) as rank2 from cpl_clean_alltrainset where(hour=7 or hour=14 or hour=17)) b 
--on (a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month)and(a.hour=b.houradd1)and(a.minute=b.minute))t1; 

drop table if exists cpl_rank2;
create table if not exists cpl_rank2 as 
select *,hour+1 as houradd1, row_number() over(partition by year,month,day,hour ORDER BY travel_time) as rank2 from cpl_clean_alltrainset 
where(hour=7 or hour=14 or hour=17);

drop table if exists cpl_featuretest3addrank2;
create table if not exists cpl_featuretest3addrank2 as
select a.*,b.rank2 from pljiaotong_featuretest3 a
left outer join cpl_rank2 b
on a.link_id =b.link_id and a.year =b.year and a.month =b.month and a.hour =b.houradd1 and a.minute =b.minute;


----------------------------------------------------------
--9.2前一小时，前两小时分钟衰减的和中位数，众数
--做8点的基本特征（7点>=58,48,38,28,18的均值，最大值，中位数，标准差）
create table cpl_temp81 as
select a.*,b.av7_travel_time
from (select * from lzh_all_training_set_addtest where ((year=2017)and(hour=8)))a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av7_travel_time from lzh_all_training_set_addtest c
where ((c.hour=7 or c.hour=6 )and(c.minute>=58)and(c.year=2017))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

create table lzh_temp82 as
select a.*,b.av748_travel_time,b.median748_travel_time,b.max748_travel_time,b.std748_travel_time
from lzh_temp81 a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av748_travel_time,median(c.travel_time) as median748_travel_time,
max(c.travel_time) as max748_travel_time,stddev(c.travel_time) as std748_travel_time
from lzh_all_training_set_addtest c where ((c.hour=7)and(c.minute>=48)and(c.year=2017))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

create table lzh_temp83 as
select a.*,b.av738_travel_time,b.median738_travel_time,b.max738_travel_time,b.std738_travel_time
from lzh_temp82 a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av738_travel_time,median(c.travel_time) as median738_travel_time,
max(c.travel_time) as max738_travel_time,stddev(c.travel_time) as std738_travel_time
from lzh_all_training_set_addtest c where ((c.hour=7)and(c.minute>=38)and(c.year=2017))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

create table lzh_temp84 as
select a.*,b.av728_travel_time,b.median728_travel_time,b.max728_travel_time,b.std728_travel_time
from lzh_temp83 a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av728_travel_time,median(c.travel_time) as median728_travel_time,
max(c.travel_time) as max728_travel_time,stddev(c.travel_time) as std728_travel_time
from lzh_all_training_set_addtest c where ((c.hour=7)and(c.minute>=28)and(c.year=2017))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

create table lzh_temp85 as
select a.*,b.av718_travel_time,b.median718_travel_time,b.max718_travel_time,b.std718_travel_time
from lzh_temp84 a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av718_travel_time,median(c.travel_time) as median718_travel_time,
max(c.travel_time) as max718_travel_time,stddev(c.travel_time) as std718_travel_time
from lzh_all_training_set_addtest c where ((c.hour=7)and(c.minute>=18)and(c.year=2017))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

drop table lzh_temp81;
drop table lzh_temp82;
drop table lzh_temp83;
drop table lzh_temp84;

--做15点的特征
create table lzh_temp151 as
select a.*,b.av758_travel_time
from (select * from lzh_all_training_set_addtest where ((year=2017)and(hour=15)))a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av758_travel_time from lzh_all_training_set_addtest c
where ((c.hour=14)and(c.minute>=58)and(c.year=2017))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

create table lzh_temp152 as
select a.*,b.av748_travel_time,b.median748_travel_time,b.max748_travel_time,b.std748_travel_time
from lzh_temp151 a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av748_travel_time,median(c.travel_time) as median748_travel_time,
max(c.travel_time) as max748_travel_time,stddev(c.travel_time) as std748_travel_time
from lzh_all_training_set_addtest c where ((c.hour=14)and(c.minute>=48)and(c.year=2017))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));


create table lzh_temp153 as
select a.*,b.av738_travel_time,b.median738_travel_time,b.max738_travel_time,b.std738_travel_time
from lzh_temp152 a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av738_travel_time,median(c.travel_time) as median738_travel_time,
max(c.travel_time) as max738_travel_time,stddev(c.travel_time) as std738_travel_time
from lzh_all_training_set_addtest c where ((c.hour=14)and(c.minute>=38)and(c.year=2017))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

create table lzh_temp154 as
select a.*,b.av728_travel_time,b.median728_travel_time,b.max728_travel_time,b.std728_travel_time
from lzh_temp153 a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av728_travel_time,median(c.travel_time) as median728_travel_time,
max(c.travel_time) as max728_travel_time,stddev(c.travel_time) as std728_travel_time
from lzh_all_training_set_addtest c where ((c.hour=14)and(c.minute>=28)and(c.year=2017))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

create table lzh_temp155 as
select a.*,b.av718_travel_time,b.median718_travel_time,b.max718_travel_time,b.std718_travel_time
from lzh_temp154 a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av718_travel_time,median(c.travel_time) as median718_travel_time,
max(c.travel_time) as max718_travel_time,stddev(c.travel_time) as std718_travel_time
from lzh_all_training_set_addtest c where ((c.hour=14)and(c.minute>=18)and(c.year=2017))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

drop table lzh_temp151;
drop table lzh_temp152;
drop table lzh_temp153;
drop table lzh_temp154;

--做18点的特征
create table lzh_temp181 as
select a.*,b.av758_travel_time
from (select * from lzh_all_training_set_addtest where ((year=2017)and(hour=18)))a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av758_travel_time from lzh_all_training_set_addtest c
where ((c.hour=17)and(c.minute>=58)and(c.year=2017))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

create table lzh_temp182 as
select a.*,b.av748_travel_time,b.median748_travel_time,b.max748_travel_time,b.std748_travel_time
from lzh_temp181 a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av748_travel_time,median(c.travel_time) as median748_travel_time,
max(c.travel_time) as max748_travel_time,stddev(c.travel_time) as std748_travel_time
from lzh_all_training_set_addtest c where ((c.hour=17)and(c.minute>=48)and(c.year=2017))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

create table lzh_temp183 as
select a.*,b.av738_travel_time,b.median738_travel_time,b.max738_travel_time,b.std738_travel_time
from lzh_temp182 a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av738_travel_time,median(c.travel_time) as median738_travel_time,
max(c.travel_time) as max738_travel_time,stddev(c.travel_time) as std738_travel_time
from lzh_all_training_set_addtest c where ((c.hour=17)and(c.minute>=38)and(c.year=2017))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

create table lzh_temp184 as
select a.*,b.av728_travel_time,b.median728_travel_time,b.max728_travel_time,b.std728_travel_time
from lzh_temp183 a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av728_travel_time,median(c.travel_time) as median728_travel_time,
max(c.travel_time) as max728_travel_time,stddev(c.travel_time) as std728_travel_time
from lzh_all_training_set_addtest c where ((c.hour=17)and(c.minute>=28)and(c.year=2017))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

create table lzh_temp185 as
select a.*,b.av718_travel_time,b.median718_travel_time,b.max718_travel_time,b.std718_travel_time
from lzh_temp184 a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av718_travel_time,median(c.travel_time) as median718_travel_time,
max(c.travel_time) as max718_travel_time,stddev(c.travel_time) as std718_travel_time
from lzh_all_training_set_addtest c where ((c.hour=17)and(c.minute>=18)and(c.year=2017))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

drop table lzh_temp181;
drop table lzh_temp182;
drop table lzh_temp183;
drop table lzh_temp184;


select count(link_id) from lzh_temp85;
select count(link_id) from lzh_temp155;
select count(link_id) from lzh_temp185;


create table lzh_feature as select * from lzh_temp85;
insert into table lzh_feature select * from lzh_temp155;
insert into table lzh_feature select * from lzh_temp185;

drop table lzh_temp85;
drop table lzh_temp155;
drop table lzh_temp185;

create table temp as select *,weekday(time_start)+1 as week from lzh_feature;
drop table lzh_feature;
create table lzh_feature as select * from temp;
drop table temp;

create table temp as
select a.*,b.length,b.width,b.link_class from lzh_feature a left outer join gy_cmp_link_static_info b on a.link_id=b.link_id;
drop table lzh_feature;
create table lzh_feature as 
select *,ln(travel_time+1) as ln1p_travel_time from temp;
drop table temp;

--------------------------------------------------------------
--train利用4/5月预测6月，8,15,18点同时段的特征
--第一个15分钟
--6月前7天
create table cpl_tempmoban as 
select link_id,hour,minute,travel_time
from lzh_all_training_set where ((month=6)or(month=5)or(month=4))and(year=2017)and((hour=8)or(hour=15)or(hour=18))--把句中or(month=5)or(month=4)删掉特征merge上来
group by link_id,hour,minute,travel_time;
--近7天中位数，方差
drop table if exists cpl_temp7D;
create table if not exists cpl_temp7D as 
select link_id,hour,minute,median(travel_time) as mea_7d,stddev(travel_time) as std_7d
from lzh_all_training_set where (month=5)and(year=2017)and((hour=8)or(hour=15)or(hour=18))and( day>22 and day<31)
group by link_id,hour,minute;--,sum((travel_time-avg(travel_time))/stddev( travel_time)) as kurt_time 

--求众数
create table cpl_tempmode7D as 
select link_id,hour,minute,travel_time,count(travel_time) as count_time 
from lzh_all_training_set where (month=5)and(year=2017)and((hour=8)or(hour=15)or(hour=18))and( day>22 and day<31)
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
drop table if exists cpl_hengxiang7d;
create table if not exists cpl_hengxiang7d as 
select a.*,b.mode_travel_time7d  from cpl_temp7D a join cpl_mode7D b
on (a.link_id =b.link_id)and(a.minute =b.minute)and(a.hour =b.hour);
--select count(*) from cpl_hengxiang7d;

drop table cpl_temp7D;
drop table cpl_mode7D;

--近15天
drop table if exists cpl_temp15D;
create table if not exists cpl_temp15D as 
select link_id,hour,minute,median(travel_time) as mea_15d,stddev(travel_time) as std_15d,avg(travel_time) as av_15d
from lzh_all_training_set where (month=5)and(year=2017)and((hour=8)or(hour=15)or(hour=18))and( day>15 and day<31)
group by link_id,hour,minute;--,sum((travel_time-avg(travel_time))/stddev( travel_time)) as kurt_time 

--求众数
create table cpl_tempmode15D as 
select link_id,hour,minute,travel_time,count(travel_time) as count_time 
from lzh_all_training_set where (month=5)and(year=2017)and((hour=8)or(hour=15)or(hour=18))and( day>15 and day<31)
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
drop table if exists cpl_hengxiang15d;
create table if not exists cpl_hengxiang15d as 
select a.*,b.mode_travel_time15d  from cpl_temp15D a join cpl_mode15D b
on (a.link_id =b.link_id)and(a.minute =b.minute)and(a.hour =b.hour);
--select count(*) from cpl_hengxiang7d;

drop table cpl_temp15D;
drop table cpl_mode15D;

--近30天
drop table if exists cpl_temp30D;
create table if not exists cpl_temp30D as 
select link_id,hour,minute,median(travel_time) as mea_30d,stddev(travel_time) as std_30d,avg(travel_time) as av_30d
from lzh_all_training_set where (month=5)and(year=2017)and((hour=8)or(hour=15)or(hour=18))and( day>0 and day<31)
group by link_id,hour,minute;--,sum((travel_time-avg(travel_time))/stddev( travel_time)) as kurt_time 

--求众数
create table cpl_tempmode30D as 
select link_id,hour,minute,travel_time,count(travel_time) as count_time 
from lzh_all_training_set where (month=5)and(year=2017)and((hour=8)or(hour=15)or(hour=18))and( day>0 and day<31)
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
drop table if exists cpl_hengxiang30d;
create table if not exists cpl_hengxiang30d as 
select a.*,b.mode_travel_time30d  from cpl_temp30D a join cpl_mode30D b
on (a.link_id =b.link_id)and(a.minute =b.minute)and(a.hour =b.hour);

drop table cpl_temp30D;
drop table cpl_mode30D;

--近60天
drop table if exists cpl_temp60D;
create table if not exists cpl_temp60D as 
select link_id,hour,minute,median(travel_time) as mea_60d,stddev(travel_time) as std_60d,avg(travel_time) as av_60d
from lzh_all_training_set where (month=5)or(month=4)and(year=2017)and((hour=8)or(hour=15)or(hour=18))and(day>0 and day<31)
group by link_id,hour,minute;--,sum((travel_time-avg(travel_time))/stddev( travel_time)) as kurt_time 

--求众数
create table cpl_tempmode60D as 
select link_id,hour,minute,travel_time,count(travel_time) as count_time 
from lzh_all_training_set where (month=5)or(month=4)and(year=2017)and((hour=8)or(hour=15)or(hour=18))and( day>0 and day<31)
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
drop table if exists cpl_hengxiang60d;
create table if not exists cpl_hengxiang60d as 
select a.*,b.mode_travel_time60d  from cpl_temp60D a join cpl_mode60D b
on (a.link_id =b.link_id)and(a.minute =b.minute)and(a.hour =b.hour);
--select count(*) from cpl_hengxiang7d;

drop table cpl_temp60D;
drop table cpl_mode60D;
-----------
--生成6月份的线上训练特征表
drop table if exists cpl_6online_train;
create table if not exists cpl_6online_train as 
select a.link_id,a.year,a.month,a.day,a.hour,a.minute,a.travel_time,e.mea_7d ,e.std_7d ,e.mode_travel_time7d
,d.mea_15d ,d.std_15d ,d.av_15d ,d.mode_travel_time15d ,d.av_15d - d.std_15d as sub15
,c.mea_30d ,c.std_30d ,c.av_30d ,c.mode_travel_time30d,c.av_30d - c.std_30d as sub30
,b.mea_60d ,b.std_60d ,b.av_60d ,b.mode_travel_time60d,b.av_60d - b.std_60d as sub60
from (select * from lzh_all_training_set where (month=6)and(year=2017)and((hour=8)or(hour=15)or(hour=18))) a 
left outer join cpl_hengxiang60d b
on (a.link_id =b.link_id)and(a.minute =b.minute)and(a.hour =b.hour)
left outer join cpl_hengxiang30d c
on (a.link_id =c.link_id)and(a.minute =c.minute)and(a.hour =c.hour)
left outer join cpl_hengxiang15d d
on (a.link_id =d.link_id)and(a.minute =d.minute)and(a.hour =d.hour)
left outer join cpl_hengxiang7d e
on (a.link_id =e.link_id)and(a.minute =e.minute)and(a.hour =e.hour)
;
--最近5天全时段statistics
--近5天中位数，方差
drop table if exists cpl_all5D;
create table if not exists cpl_all5D as 
select link_id,median(travel_time) as mea_5d,stddev(travel_time) as std_5d,sum(travel_time) as sum_5d,avg(travel_time) as avg_5d
from lzh_all_training_set where (month=5)and(year=2017)and( day>24 and day<31)
group by link_id;--,sum((travel_time-avg(travel_time))/stddev( travel_time)) as kurt_time 
--最近30天对应时段衰减累积流量
--近30天中位数，方差
drop table if exists cpl_hengxiangex30D;
create table if not exists cpl_hengxiangex30D as 
select link_id,hour,median(travel_time) as mea_30exd,stddev(travel_time) as std_30exd,sum(travel_time) as sum_30exd,avg(travel_time) as avg_30exd --jiaminute纵向
from (select link_id,hour,minute,travel_time*exp(30-day) as travel_time from lzh_all_training_set where (month=5)and(year=2017)and(day>0 and day<31)) t1
group by link_id,hour;
--加上新的全时段和30天时间衰减的新的线上训练表
drop table if exists cpl_6online_train_ex;
create table if not exists cpl_6online_train_ex as 
select a.*,ln(a.travel_time+1)as ln_travel_time, b.mea_5d ,b.std_5d ,b.sum_5d ,b.avg_5d,c.mea_5d as mea_30exd,c.std_5d as std_30exd,c.sum_5d as sum_30exd,c.avg_5d as avg_30exd 
from cpl_6online_train a
left outer join cpl_all5D b
on a.link_id=b.link_id 
left outer join cpl_hengxiangex30D c
on a.link_id =c.link_id and a.hour =c.hour 

-------------
--线上特征
create table if not exists ttemp as 
select * from gy_cmp_training_traveltime 
where ( to_date(date_time, 'yyyy-mm-dd')  > to_date('20170701', 'yyyymmdd') );
read ttemp;
select count(*) from ttemp; 
--从辉哥lzhfeature表获得别人提供的基础特征（线上的测试集）
drop table if exists pljiaotong_featuretest;
create table if not exists  pljiaotong_featuretest as 
select * from lzh_feature; 

--cpl_feature加入speed特征（不含节日）
--create table temp as select *, weekofyear(time_start) as weekyear from cpl_feature ;
drop table if exists cpl_feature_addspeed;
create table cpl_feature_addspeed as select * ,weekofyear(time_start) as weekyear, av718_travel_time/length as speed1, median718_travel_time/length as speed2 
,av728_travel_time/length as speed3
, median728_travel_time/length as speed4
from cpl_feature;
--drop table temp;

--drop table if exists cpl_feature_addspeed;
--create table cpl_feature_addspeed as select * from cpl_feature;
--insert into table cpl_feature_addspeed  select * from cpl_feature;

create table cpl_temptotal as --8点的累积旅行时间
select a.*, b.av800_travel_time,b.sum800_travel_time
from (select * from lzh_all_training_set_addtest where ((year=2017)and(hour=8))) a 
left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time)as av800_travel_time,sum(c.travel_time) as sum800_travel_time from lzh_all_training_set_addtest c
where ((c.hour=8)and(c.year=2017))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

--节假日
DROP TABLE IF EXISTS cpl_jiaotng_date_features;
CREATE TABLE IF NOT EXISTS cpl_jiaotng_date_features AS
SELECT
	time_start
	,dt
	,day_index
	,month_index
	,year_index
	--,month
	--,day
	,(month*100+day) as month_day
	,(year*100+month) as year_month
	,case when (weekday in (6,7) and special_workday == 0) or holiday==1 then 0 else 1 end as workday
	,weekofyear
	,day_to_lastday
	,month_day_num
	,weekday
	,holiday
	,special_workday
	,special_holiday
	,day1_before_special_holiday
	,day2_before_special_holiday
	,day3_before_special_holiday
	,day1_before_holiday
	,day2_before_holiday
	,day3_before_holiday
	,day1_after_special_holiday
	,day2_after_special_holiday
	,day3_after_special_holiday
	,day1_after_holiday
	,day2_after_holiday
	,day3_after_holiday
FROM
(
	SELECT
		time_start
		,dt
		,datediff(dt,to_date('2016-01-01','yyyy-mm-dd'),'dd')+1 as day_index
		,datediff(dt,to_date('2016-01-01','yyyy-mm-dd'),'mm')+1 as month_index
		,datepart(dt,'yyyy')-2016+1 as year_index
		,datepart(dt,'yyyy') as year
		,datepart(dt,'mm') as month
		,datepart(dt,'dd') as day
		,datepart(lastday(dt),'dd') as month_day_num
		,weekofyear(dt) as weekofyear
		,datediff(lastday(dt),dt,'dd') as day_to_lastday
		,weekday(dt) as weekday
		,holiday
		,special_workday
		,special_holiday
		
		,case when cast(to_char(dateadd(dt,-1,'dd'),'yyyymmdd') as bigint) in (20170404,20170501,20170514,20170530) then 1 else 0 end as day1_before_special_holiday
		,case when cast(to_char(dateadd(dt,-2,'dd'),'yyyymmdd') as bigint) in (20170404,20170501,20170514,20170530)then 1 else 0 end as day2_before_special_holiday
		,case when cast(to_char(dateadd(dt,-3,'dd'),'yyyymmdd') as bigint) in (20170404,20170501,20170514,20170530) then 1 else 0 end as day3_before_special_holiday
		
		,case when cast(to_char(dateadd(dt,-1,'dd'),'yyyymmdd') as bigint) in (20170404,20170501,20170514,20170530) then 1 else 0 end as day1_before_holiday
		,case when cast(to_char(dateadd(dt,-2,'dd'),'yyyymmdd') as bigint) in (20170404,20170501,20170514,20170530) then 1 else 0 end as day2_before_holiday
		,case when cast(to_char(dateadd(dt,-3,'dd'),'yyyymmdd') as bigint) in (20170404,20170501,20170514,20170530) then 1 else 0 end as day3_before_holiday
		
		,case when cast(to_char(dateadd(dt,1,'dd'),'yyyymmdd') as bigint) in (20170404,20170501,20170514,20170530) then 1 else 0 end as day1_after_special_holiday
		,case when cast(to_char(dateadd(dt,2,'dd'),'yyyymmdd') as bigint) in (20170404,20170501,20170514,20170530) then 1 else 0 end as day2_after_special_holiday
		,case when cast(to_char(dateadd(dt,3,'dd'),'yyyymmdd') as bigint) in (20170404,20170501,20170514,20170530) then 1 else 0 end as day3_after_special_holiday
		
		,case when cast(to_char(dateadd(dt,1,'dd'),'yyyymmdd') as bigint) in (20170404,20170501,20170514,20170530) then 1 else 0 end as day1_after_holiday
		,case when cast(to_char(dateadd(dt,2,'dd'),'yyyymmdd') as bigint) in (20170404,20170501,20170514,20170530) then 1 else 0 end as day2_after_holiday
		,case when cast(to_char(dateadd(dt,3,'dd'),'yyyymmdd') as bigint) in (20170404,20170501,20170514,20170530) then 1 else 0 end as day3_after_holiday
		
	FROM
	(
		SELECT
			time_start
			,to_date(to_char(time_start,'yyyymmdd'),'yyyymmdd') as dt
			,case when (year*10000+month*100+day) in (20170402,20170403,20170404,20170429,20170430,20170501,20170528,20170529,20170530) then 1 else 0 end as holiday
			,case when (year*10000+month*100+day) in (20170401,20170527) then 1 else 0 end as special_workday
			,case when (year*10000+month*100+day) in (20170404,20170501,20170514,20170530) then 1 else 0 end as special_holiday
			
		FROM
			lzh_all_training_set_addtest 	
	)t1
)t2
;
drop table if exists  pljiaotong_featuretest2;
create table if not exists pljiaotong_featuretest2 as select a.*,b.day_index ,b.month_index ,b.workday ,b.weekofyear ,b.day_to_lastday ,b.month_day_num ,b.holiday 
,b.special_workday ,b.special_holiday ,b.day1_before_special_holiday ,b.day2_before_special_holiday ,b.day3_before_special_holiday ,b.day1_before_holiday ,b.day2_before_holiday 
,b.day3_before_holiday ,b.day1_after_special_holiday ,b.day2_after_special_holiday ,b.day3_after_special_holiday ,b.day1_after_holiday ,b.day2_after_holiday ,b.day3_after_holiday 
from pljiaotong_featuretest a
left outer join cpl_jiaotng_date_features b 
on a.time_start =b.time_start
;
select count(*) from pljiaotong_featuretest2;
select count(*) from pljiaotong_featuretest;
select count(*) from cpl_jiaotng_date_features;--节假日;
select count(distinct link_id,time_start ) from pljiaotong_featuretest2;

drop table if exists  pljiaotong_featuretest3;
create table if not exists pljiaotong_featuretest3 as 
select s.*  
from (
    select *, row_number() over (partition by link_id,time_start order by day) as group_idx  
    from pljiaotong_featuretest2 
) s
where s.group_idx = 1;
select count(*) from pljiaotong_featuretest3;

--结果提交
--alter table gy_cmp_testing_seg1 rename to lzh0826sub;

drop table if exists temp;
drop table if exists temp11;


truncate table gy_cmp_testing_seg1; 
drop table if exists gy_cmp_testing_seg1;
create table gy_cmp_testing_seg1 as
select a.link_id,a.date_time,a.time_interval, exp(b.prediction_result)-1 as travel_time from lzh_testing_seg1 a join cpl_longterm_online b
on (a.link_id =b.link_id)and(a.minute =b.minute)and(a.hour =b.hour)and(a.day =b.day);

--select count(*) from gy_cmp_testing_seg1 ;
drop table if exists temp12;
create table if not exists temp12 as
select count(*) from gy_cmp_testing_seg1 where travel_time>0;
read gy_cmp_testing_seg1;
read cpl_testset_result3;
select count(*) from temp11  where link_id='4377906289869500514' and  month=7 and year=2017 and hour=8 and day=3 and prediction_result>0;
select count(*) from temp  where  prediction_result>0;
select count(*) from pljiaotong_featuretest3 where month=7 and year=2017 and hour=15 ;

select count(travel_time) from lzh_all_training_set  where cast(travel_time as double )<1 and hour=8 ;
