package com.learn.hive.code.loadData;

/*
    create table student(id int, name string);

    show tables;

    insert into table student values(1,'hell10');

    select * from student;

    load data local inpath '/opt/com.learn.hive/data/data.txt' into table student;

    select count(1) from student;

    create table stud(id int, name string) row format delimited fields terminated by ',';

    create table stud2(id int, name string) row format delimited fields terminated by ',';

    load data local inpath '/opt/com.learn.hive/data/data2.txt' into table stud2;

    // 有时候会出现NULL值，是因为分隔符号有问题

    存储在 hdfs 上的路径是 /user/com.learn.hive/warehouse/stud2
    可以直接给这个路径上 put 文件，然后hive 里面
    select * from stud2;
    就可以看到了put 到 hdfs 上的数据

    在 hdfs 的根目录放一个 data4.txt 文件
    com.learn.hive 中执行
    load data inpath '/data4.txt' into table stud2;
    select * from stud2;
    可以看到加载进来的数据

    create table test(
        name string,
        friends array<String>,
        children map<String, int>,
        address struct<street:string, city: string>
    )
    row format delimited fields terminated by ','
    collection items terminated by '_'
    map keys terminated by ':'
    lines terminated by '\n';

songsong,bingbing_lili, xiao song:18_xiaoxiao song:19, hui long guan_beijing
yangyang,caicai_susu, xiao yang:18_xiaoxiao song:19,chao yang_beijing
    ;
 */
public class command {
}
