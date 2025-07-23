create database ecommerce;
use ecommerce;
create table category(id int primary key auto_increment,catname varchar(100),catid int);
select * from category;

--- first level
select * from category where catid=0;

--- second level
select * from category where catid in (select id from category where catid=0);