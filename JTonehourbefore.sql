--做8点的基本特征（7点>=58,48,38,28,18的均值，最大值，中位数）
create table cpl_temp81 as --获得17年4~6月的每天早上8点前2分的av、max、mode
select a.*,b.av758_travel_time
from (select * from lzh_all_training_set_addtest where ((year=2017)and(hour=8)))a left outer join--这句话中(year=2017)可以删了？只取2017年8点的数据，然后把这一小时前的特征合并上去
(select c.link_id,c.day,c.month,avg(c.travel_time) as av758_travel_time from lzh_all_training_set_addtest c
where ((c.hour=7)and(c.minute>=58))--
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

create table cpl_temp82 as
select a.*,b.av748_travel_time,b.median748_travel_time,b.max748_travel_time,b.std748_travel_time
from cpl_temp81 a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av748_travel_time,median(c.travel_time) as median748_travel_time,
max(c.travel_time) as max748_travel_time,stddev(c.travel_time) as std748_travel_time
from lzh_all_training_set_addtest c where ((c.hour=7)and(c.minute>=48))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

create table cpl_temp83 as
select a.*,b.av738_travel_time,b.median738_travel_time,b.max738_travel_time,b.std738_travel_time
from cpl_temp82 a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av738_travel_time,median(c.travel_time) as median738_travel_time,
max(c.travel_time) as max738_travel_time,stddev(c.travel_time) as std738_travel_time
from lzh_all_training_set_addtest c where ((c.hour=7)and(c.minute>=38))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

create table cpl_temp84 as
select a.*,b.av728_travel_time,b.median728_travel_time,b.max728_travel_time,b.std728_travel_time
from cpl_temp83 a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av728_travel_time,median(c.travel_time) as median728_travel_time,
max(c.travel_time) as max728_travel_time,stddev(c.travel_time) as std728_travel_time
from lzh_all_training_set_addtest c where ((c.hour=7)and(c.minute>=28))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

create table cpl_temp85 as
select a.*,b.av718_travel_time,b.median718_travel_time,b.max718_travel_time,b.std718_travel_time
from cpl_temp84 a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av718_travel_time,median(c.travel_time) as median718_travel_time,
max(c.travel_time) as max718_travel_time,stddev(c.travel_time) as std718_travel_time
from lzh_all_training_set_addtest c where ((c.hour=7)and(c.minute>=18))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

drop table cpl_temp81;
drop table cpl_temp82;
drop table cpl_temp83;
drop table cpl_temp84;

--做15点的特征
create table cpl_temp151 as
select a.*,b.av758_travel_time
from (select * from lzh_all_training_set_addtest where ((year=2017)and(hour=15)))a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av758_travel_time from lzh_all_training_set_addtest c
where ((c.hour=14)and(c.minute>=58))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

create table cpl_temp152 as
select a.*,b.av748_travel_time,b.median748_travel_time,b.max748_travel_time,b.std748_travel_time
from cpl_temp151 a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av748_travel_time,median(c.travel_time) as median748_travel_time,
max(c.travel_time) as max748_travel_time,stddev(c.travel_time) as std748_travel_time
from lzh_all_training_set_addtest c where ((c.hour=14)and(c.minute>=48))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

create table cpl_temp153 as
select a.*,b.av738_travel_time,b.median738_travel_time,b.max738_travel_time,b.std738_travel_time
from cpl_temp152 a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av738_travel_time,median(c.travel_time) as median738_travel_time,
max(c.travel_time) as max738_travel_time,stddev(c.travel_time) as std738_travel_time
from lzh_all_training_set_addtest c where ((c.hour=14)and(c.minute>=38))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

create table cpl_temp154 as
select a.*,b.av728_travel_time,b.median728_travel_time,b.max728_travel_time,b.std728_travel_time
from cpl_temp153 a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av728_travel_time,median(c.travel_time) as median728_travel_time,
max(c.travel_time) as max728_travel_time,stddev(c.travel_time) as std728_travel_time
from lzh_all_training_set_addtest c where ((c.hour=14)and(c.minute>=28))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

create table cpl_temp155 as
select a.*,b.av718_travel_time,b.median718_travel_time,b.max718_travel_time,b.std718_travel_time
from cpl_temp154 a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av718_travel_time,median(c.travel_time) as median718_travel_time,
max(c.travel_time) as max718_travel_time,stddev(c.travel_time) as std718_travel_time
from lzh_all_training_set_addtest c where ((c.hour=14)and(c.minute>=18))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

drop table cpl_temp151;
drop table cpl_temp152;
drop table cpl_temp153;
drop table cpl_temp154;

--做18点的特征
create table cpl_temp181 as
select a.*,b.av758_travel_time
from (select * from lzh_all_training_set_addtest where ((year=2017)and(hour=18)))a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av758_travel_time from lzh_all_training_set_addtest c
where ((c.hour=17)and(c.minute>=58))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

create table cpl_temp182 as
select a.*,b.av748_travel_time,b.median748_travel_time,b.max748_travel_time,b.std748_travel_time
from cpl_temp181 a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av748_travel_time,median(c.travel_time) as median748_travel_time,
max(c.travel_time) as max748_travel_time,stddev(c.travel_time) as std748_travel_time
from lzh_all_training_set_addtest c where ((c.hour=17)and(c.minute>=48))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

create table cpl_temp183 as
select a.*,b.av738_travel_time,b.median738_travel_time,b.max738_travel_time,b.std738_travel_time
from cpl_temp182 a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av738_travel_time,median(c.travel_time) as median738_travel_time,
max(c.travel_time) as max738_travel_time,stddev(c.travel_time) as std738_travel_time
from lzh_all_training_set_addtest c where ((c.hour=17)and(c.minute>=38))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

create table cpl_temp184 as
select a.*,b.av728_travel_time,b.median728_travel_time,b.max728_travel_time,b.std728_travel_time
from cpl_temp183 a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av728_travel_time,median(c.travel_time) as median728_travel_time,
max(c.travel_time) as max728_travel_time,stddev(c.travel_time) as std728_travel_time
from lzh_all_training_set_addtest c where ((c.hour=17)and(c.minute>=28))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

create table cpl_temp185 as
select a.*,b.av718_travel_time,b.median718_travel_time,b.max718_travel_time,b.std718_travel_time
from cpl_temp184 a left outer join
(select c.link_id,c.day,c.month,avg(c.travel_time) as av718_travel_time,median(c.travel_time) as median718_travel_time,
max(c.travel_time) as max718_travel_time,stddev(c.travel_time) as std718_travel_time
from lzh_all_training_set_addtest c where ((c.hour=17)and(c.minute>=18))
group by c.link_id,c.day,c.month)b
on ((a.link_id=b.link_id)and(a.day=b.day)and(a.month=b.month));

drop table cpl_temp181;
drop table cpl_temp182;
drop table cpl_temp183;
drop table cpl_temp184;


select count(link_id) from cpl_temp85;
select count(link_id) from cpl_temp155;
select count(link_id) from cpl_temp185;


create table cpl_feature as select * from cpl_temp85;
insert into table cpl_feature select * from cpl_temp155;
insert into table cpl_feature select * from cpl_temp185;

drop table cpl_temp85;
drop table cpl_temp155;
drop table cpl_temp185;

create table temp as select *,weekday(time_start)+1 as week from cpl_feature;
drop table cpl_feature;
create table cpl_feature as select * from temp;
drop table temp;

create table temp as
select a.*,b.length,b.width,b.link_class from cpl_feature a left outer join gy_cmp_link_static_info b on a.link_id=b.link_id;
drop table cpl_feature;
create table cpl_feature as 
select *,ln(travel_time+1) as ln1p_travel_time from temp;
drop table temp;