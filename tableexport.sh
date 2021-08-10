#!/bin/sh
export HADOOP_USER_NAME=hdfs
export_hiveserver=172.18.9.45
export_username=hive
export_passwd=123456
export_database=default
export_table=bank_info_1
export_conn_url='jdbc:hive2://'${export_hiveserver}':10000/'${export_database}
export_nn_ip=172.18.9.45
import_nn_ip=172.18.9.52
import_hiveserver=172.18.9.52
import_username=hive
import_passwd=123456
import_database=default
import_conn_url='jdbc:hive2://'${import_hiveserver}':10000/'${import_database}
currentdate=`date +%Y-%m-%d`
#创建脚本临时文件存放目录
if [ ! -d "/tmp/${export_table}${currentdate}" ]; then
mkdir /tmp/${export_table}${currentdate}
fi
dir=/tmp/${export_table}${currentdate}
#将表还原成txt表语句
echo "create table ${export_table}_export location '/tmp/${export_table}_export/'  as select * from ${export_table};" >${dir}/create_table.sql
#将txt表数据插入目标表语句
echo "insert into ${export_table} select * from ${export_table}_export;" > ${dir}/import_table.sql

echo 'step 1:export table 备份表结构'>>${dir}/exporttable_${export_table}.log
beeline -u ${export_conn_url} -n ${export_username} -p ${export_passwd} -e "SHOW create table ${export_table};" >${dir}/tabledesc_${export_table}.sql
beeline -u ${export_conn_url} -e "SHOW create table ${export_table};" --showHeader=false --maxWidth=1000 >${dir}/tabledesc_${export_table}.sql
# 去除行首/尾的 '|'
sed -i 's/^.//g;s/.$//g' ${dir}/tabledesc_${export_table}.sql
# 去除行尾空格
sed -i 's/ *$//' ${dir}/tabledesc_${export_table}.sql
# 补全定义结束符 '/'
sed -i '/CREATE OR REPLACE/i\/' ${dir}/tabledesc_${export_table}.sql
# 去除前1-3行
sed -i '1,3d' ${dir}/tabledesc_${export_table}.sql
# 去除最后一行
sed -i '$d' ${dir}/tabledesc_${export_table}.sql
# 去除location所在一行及下一行
sed -i '/LOCATION/,+1d' ${dir}/tabledesc_${export_table}.sql

echo 'step 2:export table 创建txt表并导入数据 '>>${dir}/exporttable_${export_table}.log
beeline -u ${export_conn_url} -f ${dir}/create_table.sql >> ${dir}/exporttable_${export_table}.log 2>&1

echo 'step 3:translate date 拷贝数据到新集群'>>${dir}/exporttable_${export_table}.log
sudo -u hdfs hadoop distcp hdfs://${export_nn_ip}:8020/tmp/${export_table}_export webhdfs://${import_nn_ip}/tmp/ >>${dir}/exporttable_${export_table}.log 2>&1

echo 'step 4:create table 新集群创建原始表'>>${dir}/exporttable_${export_table}.log
beeline -u ${import_conn_url} -n ${import_username} -p ${import_passwd} -f ${dir}/tabledesc_${export_table}.sql >> ${dir}/exporttable_${export_table}.log 2>&1

echo 'step 5:create table 新集群创建txt表'>>${dir}/exporttable_${export_table}.log
#生产目标txt表ddl:①去除）后的语句
sed '/)/,$D' ${dir}/tabledesc_${export_table}.sql  >${dir}/new_tabledesc_${export_table}.sql
#生产目标txt表ddl:修改为txt表对应表名
sed -i "0,/${export_table}/s//${export_table}_export/" ${dir}/new_tabledesc_${export_table}.sql

#判断是否是分区表
partitionFlag=`grep -c "PARTITIONED BY" ${dir}/tabledesc_${export_table}.sql`

if [ "$partitionFlag" -ne "0" ]; then
#如果是分区表：①建txt表找到分区字段及类型
partitionString=`sed -n "/PARTITIONED BY/,+1{//b;p}" ${dir}/tabledesc_user_acc_level.sql | cut -d ')' -f 1`
#如果是分区表：②建txt表在表ddl之后加上分区字段及位置
echo ",${partitionString} ) location '/tmp/${export_table}_export'" >> ${dir}/new_tabledesc_${export_table}.sql
beeline -u ${import_conn_url} -n ${import_username} -p ${import_passwd} -f ${dir}/new_tabledesc_${export_table}.sql >>${dir}/exporttable_${export_table}.log 2>&1

echo 'step 5:create table 分区表还原数据'>>${dir}/exporttable_${export_table}.log
#如果是分区表：③修改动态分区插入语句
partitionColumn=`echo ${partitionString} | cut -d ' ' -f 1`
echo -e "SET hive.exec.dynamic.partition=true;\ninsert into ${export_table} partition(${partitionColumn}) select * from ${export_table}_export;" > ${dir}/import_table.sql
beeline -u ${import_conn_url} -n ${import_username} -p ${import_passwd} -f ${dir}/import_table.sql >>${dir}/exporttable_${export_table}.log 2>&1

else
#如果不是分区表：①建txt表在表ddl后加上位置
echo ") location '/tmp/${export_table}_export'" >> ${dir}/new_tabledesc_${export_table}.sql
beeline -u ${import_conn_url} -n ${import_username} -p ${import_passwd} -f ${dir}/new_tabledesc_${export_table}.sql >>${dir}/exporttable_${export_table}.log 2>&1

echo 'step 5:create table 普通表还原数据'>>${dir}/exporttable_${export_table}.log
beeline -u ${import_conn_url} -n ${import_username} -p ${import_passwd} -f ${dir}/import_table.sql >>${dir}/exporttable_${export_table}.log
fi
