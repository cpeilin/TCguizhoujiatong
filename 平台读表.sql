--平台读表
-- 道路静态信息
create table gy_cmp_link_static_info like odps_tc_257100_f673506e024.gy_cmp_link_static_info;
-- 写入
insert overwrite  table gy_cmp_link_static_info
select * from odps_tc_257100_f673506e024.gy_cmp_link_static_info;

-- 拓扑
create table gy_cmp_link_in_out_top_info like odps_tc_257100_f673506e024.gy_cmp_link_in_out_top_info;
-- 写入
insert overwrite  table gy_cmp_link_in_out_top_info
select * from odps_tc_257100_f673506e024.gy_cmp_link_in_out_top_info;

-- 时间表
create table gy_cmp_training_traveltime like odps_tc_257100_f673506e024.gy_cmp_training_traveltime;
-- 写入
insert overwrite  table gy_cmp_training_traveltime
select * from odps_tc_257100_f673506e024.gy_cmp_training_traveltime;

-- 模板表
create table gy_cmp_testing_template_seg1 like odps_tc_257100_f673506e024.gy_cmp_testing_template_seg1;
-- 写入
insert overwrite  table gy_cmp_testing_template_seg1
select * from odps_tc_257100_f673506e024.gy_cmp_testing_template_seg1;


#read odps_tc_257100_f673506e024.gy_cmp_link_static_info;

#read odps_tc_257100_f673506e024.gy_cmp_link_in_out_top_info;

#read odps_tc_257100_f673506e024.gy_cmp_training_traveltime;