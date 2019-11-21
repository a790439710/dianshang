#!/bin/bash

# 定义变量方便修改
APP=gmall
hive=/opt/module/hive-1.2.1/bin/hive

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
	do_date=$1
else 
	do_date=`date -d "-1 day" +%F`  
fi 

sql="

add jar /opt/module/hive-1.2.1/myudf-1.0-SNAPSHOT.jar;
create temporary function base_analizer as 'com.hive.udf.BaseFieldUDF';
create temporary function flat_analizer as 'com.hive.udtf.EventJsonUDTF';

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table "$APP".dwd_base_event_log 
PARTITION (dt='$do_date')
select
mid_id,
user_id,
version_code,
version_name,
lang,
source,
os,
area,
model,
brand,
sdk_version,
gmail,
height_width,
app_time,
network,
lng,
lat,
event_name,
event_json,
server_time
from
(
select
base_analizer(line, 'cm', 'mid') as mid_id,
base_analizer(line, 'cm', 'uid') as user_id,
base_analizer(line, 'cm', 'vc') as version_code,
base_analizer(line, 'cm', 'vn') as version_name,
base_analizer(line, 'cm', 'l') as lang,
base_analizer(line, 'cm', 'sr') as source,
base_analizer(line, 'cm', 'os') as os,
base_analizer(line, 'cm', 'ar') as area,
base_analizer(line, 'cm', 'md') as model,
base_analizer(line, 'cm', 'ba') as brand,
base_analizer(line, 'cm', 'sv') as sdk_version,
base_analizer(line, 'cm', 'g') as gmail,
base_analizer(line, 'cm', 'hw') as height_width,
base_analizer(line, 'cm', 't') as app_time,
base_analizer(line, 'cm', 'nw') as network,
base_analizer(line, 'cm', 'ln') as lng,
base_analizer(line, 'cm', 'la') as lat,
base_analizer(line, 'et', '') as ops,
base_analizer(line, '', '') as server_time
from "$APP".ods_event_log where dt='$do_date'
) sdk_log lateral view flat_analizer(ops) tmp_k as event_name, event_json;"

$hive -e "$sql"
