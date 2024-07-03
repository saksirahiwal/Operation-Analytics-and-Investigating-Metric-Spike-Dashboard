show databases;
create database job_db;
use job_db;
show tables;

##-------- CASE STUDY 1--------
#---------Table1--------
create table job_detail(
ds date,
job_id int not null,
actor_id int not null,
event varchar(15) not null,
language varchar(15) not null,
time_spen int not null ,
org char(2)
);
select * from job_data1;

#1----- find jobs reviewed per hour per day-----
select 
avg(t) as "avg jobs reviewed per day per hour",
avg(P) as "avg jobs reviewed per day per secoond" 
from 
(select
ds,
((count(job_id)*3600)/sum(time_spent)) as t,
((count(job_id))/sum(time_spent)) as p
from
job_data1
where
month(ds)=11
group by ds) a;

#2-----  7-day rolling average of throughput-------
select ROUND(COUNT(event)/sum(time_spent),2) as "weekly Throughput" from job_data1;

select ds as Dates, ROUND(COUNT(event)/sum(time_spent),2) as "Daily Throughput" from job_data1 
group by ds order by ds;

#3------ calculate the percentage share of each language ----------
select language as languages ,ROUND(100* count(*)/total,2) as percentage, sub.total
from job_data1
cross join (select count(*) as total from job_data1) as sub 
group by language , sub.total;

#4------- Identify duplicate rows in the data---------
select actor_id, count(*) as duplicates from job_data1
group by actor_id having count(*)>1;

##------ CASE STUDY 2------
show databases ;
use job_db ;
# Table1 users
show tables;
create table users(
user_id int,
created_at varchar(100),
company_id int,
language varchar(50),
activated_at varchar(100),
state varchar(50)
);
select * from user_data;
alter table user_data add column temp_created datetime;
update user_data set temp_created=STR_TO_DATE(created_at ,'%d-%m-%Y %H:%i');
alter table user_data drop column created_at;
alter table user_data change column temp_created  created_at datetime;


show databases;
use job_db;

#----Table2 events-----
create table event(
user_id int,
occurred_at varchar(100),
event_type varchar(100),
event_name varchar(100),
location varchar(100),
device varchar(100),
user_type int
);

show tables;
SHOW VARIABLES LIKE 'secure_file_priv';

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv"
INTO TABLE event
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
select * from event;
alter table event add column temp_occurred datetime;
update event set temp_occurred=str_to_date(occurred_at,'%d-%m-%Y %H:%i');
alter table event drop column occurred_at ;
alter table event change column temp_occurred occurred_at datetime;
select * from event;

#--- table3 email_events----
show databases;
use job_db;

create table email_event(
user_id  int,
occurred_at varchar (100),
action varchar(100),
user_type int
);

SHOW VARIABLES LIKE 'secure_file_priv';

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv"
INTO TABLE email_event
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
select * from email_event;

alter table email_event add column temp_occurred datetime;
update email_event set temp_occurred=str_to_date(occurred_at,'%d-%m-%Y %H:%i');
alter table email_event drop column occurred_at ;
alter table email_event change column temp_occurred occurred_at datetime;

##---Task1:Weekly User Engagement----
select extract(week from occurred_at ) as week_number,
count(distinct user_id) as active_user
from event 
where event_type='engagement'
group by week_number
order by week_number;

##---- Task2:User Growth Analysis-----

select
extract(YEAR FROM user_data.created_at) as year,
extract( MONTH FROM user_data.created_at) as month,
count( DISTINCT user_data.user_id) as new_users
from user_data
group by year,month 
order by year,month;

##---Task3:Weekly Retention Analysis-----
select * from user_data;
WITH signup_cohorts AS (
  SELECT
    user_id,
    EXTRACT(YEAR FROM created_at) AS signup_year,
    EXTRACT(WEEK FROM created_at) AS signup_week
  FROM
    user_data
),
weekly_engagement AS (
  SELECT
    user_id,
    EXTRACT(YEAR FROM occurred_at) AS activity_year,
    EXTRACT(WEEK FROM occurred_at) AS activity_week
  FROM
    email_event
)
SELECT
  sc.signup_year,
  sc.signup_week,
  we.activity_year,
  we.activity_week,
  COUNT(DISTINCT we.user_id) AS retained_users
FROM
  signup_cohorts sc
LEFT JOIN
  weekly_engagement we ON sc.user_id = we.user_id
GROUP BY
  sc.signup_year, sc.signup_week, we.activity_year, we.activity_week
ORDER BY
  sc.signup_year, sc.signup_week, we.activity_year, we.activity_week;


##----Task4:Weekly Engagement Per Device------

select 
EXTRACT(YEAR FROM event.occurred_at) as year,
EXTRACT(WEEK FROM event.occurred_at) as week,
event.device,
count(DISTINCT user_data.user_id) as active_users
from event
join user_data on event.user_id=user_data.user_id
group by 
year, week,event.device
order by
year,week,event.device;

##---Task5:Email Engagement Analysis-----

select 
100*sum(case when email_cat='email_open' then 1 else 0 end)/
sum(case when email_cat='email_sent'then 1 else 0 end) as email_open_rate,
100*sum(case when email_cat='email-clicked' then 1 else 0 end)/
sum(case when email_cat='email_sent' then 1 else 0 end) as email_clicked_rate
from (select*,
case
when action in ('sent_ weekly_digest','sent_reengagement_email') then 'email_sent'
when action in ('email_open') then 'email_open'
when action in ('email_clickthrough') then 'email_clicked'
end as email_cat
from email_event)sub;

























