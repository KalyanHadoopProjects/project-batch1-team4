JSON DATA:
----------
add jar /home/prakash/work/apache-hive-1.2.1-bin/lib/hive-serdes-1.0-SNAPSHOT.jar;

create table hivetask (
    name string,
    id int,
    course string,
    year int
    ) ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDe' location '/home/prakash/work/warehouse/prakash1.db';


create table hivetask1 as select * from hivetask;

CREATE TABLE hivetaskop(name string, id int, course string, year int);

insert into hivetaskop(name,id, course, year) select name,id, course, year from hivetask1 where id>2 and course="spark";

 select * from hivetaskop;



XML DATA:
--------
add jar /home/prakash/work/apache-hive-1.2.1-bin/lib/hivexmlserde-1.0.5.3.jar;

create table hiveraw(line string);
load data local inpath '/home/prakash/student1.xml' into table hiveraw;

create table hivexml (name string,id int, course string, year int)
   row format delimited
   fields terminated by ',';

insert into table hivexml
select xpath_string(line,'student/name'),
        xpath_int(line,'student/id'),
        xpath_string(line,'student/course'),
        xpath_int(line,'student/year')
         from hiveraw;

select xpath_string(line,'student/name') from  hiveraw;

select xpath_int(line,'student/id') from hiveraw;


CREATE TABLE hivexmlop(name string, id int, course string, year int);

INSERT INTO hivexmlop(name, id, course, year) SELECT name, id, course, year FROM hivexml WHERE id>2 and course="spark";












create table hivetaskx (
    name string,
    id int,
    course string,
    year int
    ) ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe' location '/home/prakash/work/warehouse/prakash1.db';
    WITH SERDEPROPERTIES (

    "column.xpath.name"="/home/prakash/name/text()",
    "column.xpath.id"="/home/prakash/id/text()",
    "column.xpath.course"="/home/prakash/course/text()",
    "column.xpath.year"="/home/prakash/year/text()"
     )
     TBLPROPERTIES (
    "xmlinput.start"="<Document",
    "xmlinput.end"="</Document>");



