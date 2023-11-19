Use crowdfunding;

drop table crowdfunding_creator;
create table projecrowdfunding_categoryct(id int,
state varchar(500),
name varchar(1000),
country varchar(500),
creator_id varchar(500),
location_id varchar(500),
category_id varchar(500),
created_at varchar(500), 
deadline varchar(500),
updated_at varchar(500),
state_changed_at varchar(500),
successful_at varchar(500),
launched_at varchar(500),
goal varchar(500),
currency varchar(50),
currency_symbol varchar(50),
usd_pledged varchar(50),
static_usd_rate varchar(50),
backers_count varchar(50));
drop table project;
LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Crowdfunding_projects_14.csv"
INTO TABLE project
CHARACTER SET latin1
COLUMNS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(id,state,name,country,creator_id,location_id,category_id,created_at,deadline,updated_at,state_changed_at,successful_at,launched_at,goal,currency,currency_symbol,usd_pledged,static_usd_rate,backers_count);

SELECT @@sql_mode;

set @@sql_mode = "NO_ENGINE_SUBSTITUTION";

drop table crowdfunding_creator;
create table crowdfunding_creator(id int,
name varchar(1000),
chosen_currency varchar(500));

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Crowdfunding_Creator.csv"
INTO TABLE crowdfunding_creator
CHARACTER SET latin1
COLUMNS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(id,name,chosen_currency);

#properly imported
select count(*) from project;
# imported 154 original count 169
select count(*) from crowdfunding_category;
#properly imported
select count(*) from crowdfunding_creator;
#properly imported
select count(*) from crowdfunding_location;

select * from project;



use crowdfunding;

select count(*) from project;
select count(*) from crowdfunding_category;
select count(*) from crowdfunding_creator;
select count(*) from crowdfunding_location;

show tables;
CREATE TABLE country AS
SELECT
    location_id as country,
    country AS code
FROM
    project;
    
select * from country;
select * from crowdfunding_category;
select * from project;
select * from crowdfunding_creator;
select * from crowdfunding_location;
OPTIMIZE TABLE project;
#------------------------------------------created view categorys for easy access -------------------------------------------------#
create or replace view categorys as select c1.id,c1.name,c1.parent_id,c2.name as 'parent_category_name' 
from crowdfunding_category c1 left join crowdfunding_category c2 on c1.id=c2.id;

select * from categorys;

#------------------------------------------created view projects for easy access -------------------------------------------------#

create view projects as 
select id, state,name,country,creator_id,location_id,category_id,
FROM_UNIXTIME(created_at,'%d-%m-%Y') as 'created_date',
FROM_UNIXTIME(deadline,'%d-%m-%Y') as 'deadline_date',
FROM_UNIXTIME(updated_at,'%d-%m-%Y') as 'updated_date',
FROM_UNIXTIME(state_changed_at,'%d-%m-%Y') as 'state_changed_date',
FROM_UNIXTIME(successful_at,'%d-%m-%Y') as 'successful_date',
FROM_UNIXTIME(launched_at,'%d-%m-%Y') as 'launched_date',
goal,goal*static_usd_rate as 'usd_goal',
usd_pledged,
static_usd_rate,backers_count from project;

#-----------------------------------------Total No.of Projects -------------------------------------------------#

select count(id) as 'Total No.of Projects' from projects;

#-------------------------------------------Total No.of Projects based on outcome-------------------------------------------------#

with cte as (select distinct state as 'State', 
			count(*) over(partition by state) as 'State_wise_total',
			crowdfunding.total_count() as 'Total_count'
			from projects) 
select *, (state_wise_total/total_count)*100 as '%_of_total' 
from cte 
order by state_wise_total desc;

#-------------------------------------------Total No.of Projects based on location-------------------------------------------------#

with cte as (select  c.country as 'Country', c.code as 'Country_code',
			count(*) over(partition by c.country,c.code) as 'Country_total',
			crowdfunding.total_count() as 'Total_count'
			from projects p inner join country c on p.Country=c.code) 
select *, (country_total/total_count)*100 as '%_of_total' 
from cte 
order by country_total desc;

#-------------------------------------------Total No.of Projects based on category-------------------------------------------------#

with cte as (select distinct parent_id as 'Category_id', c.parent_category_name as 'Category_name',
			count(*) over(partition by parent_id,c.parent_category_name) as 'Category_wise_total',
			crowdfunding.total_count() as 'Total_count'
			from categorys c right join projects p on p.category_id=c.id) 
select Category_name,Category_wise_total
from cte  where Category_name is not NULL
order by category_id asc;

#-------------------------------------------Total No.of Projects created by year-------------------------------------------------#

with cte as(select FROM_UNIXTIME(created_at,"%Y") as 'year',count(*) as 'Year_total_count',crowdfunding.total_count() as 'Total_count'
			from project
			group by FROM_UNIXTIME(created_at,"%Y")
			order by FROM_UNIXTIME(created_at,"%Y") asc)
select *, (year_total_count/total_count)*100 as '%_of_total' 
from cte ;
#-------------------------------------------Total No.of Projects created by quarter-------------------------------------------------#

with cte as (select *,FROM_UNIXTIME(created_at,"%m") as 'Month1' from project),
cte2 as( select *, case when month1<=3 then "Q1" when month1<=6 then "Q2" when month1<=9 then "Q3" else "Q4" end as 'qutr' from cte),
cte3 as (select qutr as ' Quarter',count(*) as 'Quarter_total_count',crowdfunding.total_count() as 'Total_count' from cte2 group by qutr)
select *, (quarter_total_count/total_count)*100 as '%_of_total' 
from cte3 ;
#-------------------------------------------Total No.of Projects created by month-------------------------------------------------#

with cte as(select FROM_UNIXTIME(created_at,"%M") as 'Month',count(*) as 'month_total_count',crowdfunding.total_count() as 'Total_count'
			from project
			group by FROM_UNIXTIME(created_at,"%M"),FROM_UNIXTIME(created_at,"%m")
			order by FROM_UNIXTIME(created_at,"%m"))
select *, (month_total_count/total_count)*100 as '%_of_total' 
from cte ;
#-------------------------------------------Total No.of Projects created by year and month-------------------------------------------------#

with cte as(select FROM_UNIXTIME(created_at,"%Y-%M") as 'Year-Month',count(*) as 'yearmonth_total_count',crowdfunding.total_count() as 'Total_count'
			from project
			group by FROM_UNIXTIME(created_at,"%Y-%M"),FROM_UNIXTIME(created_at,"%Y-%m")
			order by FROM_UNIXTIME(created_at,"%Y-%m"))
select *, (yearmonth_total_count/total_count)*100 as '%_of_total' 
from cte ;
#-------------------------------------------Successful Projects amount raised-------------------------------------------------#

select sum(usd_pledged) as 'Amount raised'
from projects
where state='successful';

#-------------------------------------------Successful Projects No.of Backers count-------------------------------------------------#

select sum(backers_count) as 'No.of Backers count'
from projects
where state='successful';

#-------------------------------------------Top 3 Successful Projects based on no.of backers-------------------------------------------------#
with cte as (select name,backers_count,
			dense_rank() over(order by backers_count desc) as 'rank1' 
			from projects)
select name as 'Name',backers_count as 'Backers count', rank1 as 'Rank'  from cte where rank1 <= 3;


#-------------------------------------------Top 3 Successful Projects based on amount pledged-------------------------------------------------#

with cte as (select name, usd_pledged,
			dense_rank() over(order by usd_pledged desc) as 'rank1' 
			from projects)
select name as 'Name', usd_pledged as 'Amount raised',rank1 as 'Rank'  from cte where rank1 <= 3;

#-------------------------------------------% of Successful Projects -------------------------------------------------#

with cte as (select distinct state as 'State', 
			count(*) over(partition by state) as 'State_wise_total',
			crowdfunding.total_count() as 'Total_count'
			from projects) 
select *, (state_wise_total/total_count)*100 as '% of Success Rate' 
from cte 
where state='successful';

#-------------------------------------------% of successful Projects based on location-------------------------------------------------#
with cte as (select distinct c.country as 'Country', c.code as 'Country_code',
			count(*) over(partition by c.Country,c.code) as 'Country_total', state ,
			crowdfunding.success_count() as 'Total_count'
			from projects p inner join country c on p.Country=c.code
            where state='successful') 
select country,country_code,country_total,total_count , (country_total/total_count)*100 as '% of Success Rate'
from cte 
order by country_total desc;

#-------------------------------------------% of Successful Projects by category-------------------------------------------------#
with cte as (select distinct parent_id as 'Category_id', c.parent_category_name as 'Category_name',
			count(*) over(partition by parent_id,c.parent_category_name) as 'Category_wise_total',
			crowdfunding.success_count() as 'Total_count',state
			from categorys c right join projects p on p.category_id=c.category_id
            where state='successful') 
select category_id,category_name,category_wise_total,total_count, (category_wise_total/total_count)*100 as '% of Success Rate'
from cte 
order by category_id asc;


#-------------------------------------------% of Successful Projects created in Year-------------------------------------------------#

with cte as(select FROM_UNIXTIME(created_at,"%Y") as 'year',count(*) as 'Year_total_count',crowdfunding.success_count() as 'Total_count'
			from project
            where state='successful'
			group by FROM_UNIXTIME(created_at,"%Y")
			order by FROM_UNIXTIME(created_at,"%Y") asc)
select *, (year_total_count/total_count)*100 as '% of Success Rate' 
from cte ;

#-------------------------------------------% of successful Projects created in quarter-------------------------------------------------#

with cte as (select *,FROM_UNIXTIME(created_at,"%m") as 'Month1' from project where state='successful'),
cte2 as( select *, case when month1<=3 then "Q1" when month1<=6 then "Q2" when month1<=9 then "Q3" else "Q4" end as 'qutr' from cte),
cte3 as (select qutr as ' Quarter',count(*) as 'Quarter_total_count',crowdfunding.success_count() as 'Total_count' from cte2 group by qutr)
select *, (quarter_total_count/total_count)*100 as '% of Success Rate' 
from cte3 
order by Quarter_total_count desc;

#-------------------------------------------% of successful Projects created in month-------------------------------------------------#

with cte as(select FROM_UNIXTIME(created_at,"%M") as 'Month',count(*) as 'month_total_count',crowdfunding.success_count() as 'Total_count'
			from project
            where state='successful'
			group by FROM_UNIXTIME(created_at,"%M"),FROM_UNIXTIME(created_at,"%m")
			order by FROM_UNIXTIME(created_at,"%m"))
select *, (month_total_count/total_count)*100 as '% of Success Rate' 
from cte ;

#-------------------------------------------Total No.of Projects created by year and month-------------------------------------------------#

with cte as(select FROM_UNIXTIME(created_at,"%Y-%M") as 'Year-Month',count(*) as 'yearmonth_total_count',crowdfunding.success_count() as 'Total_count'
			from project
            where state='successful'
			group by FROM_UNIXTIME(created_at,"%Y-%M"),FROM_UNIXTIME(created_at,"%Y-%m")
			order by FROM_UNIXTIME(created_at,"%Y-%m"))
select *, (yearmonth_total_count/total_count)*100 as '% of Success Rate' 
from cte ;

#-----------------------------------------------------% of Successful Projects by goal range-------------------------------------------------#

with cte as(select crowdfunding.success_count() as 'Total_count',
		case when usd_goal<=9999 then '0-9999' 
		 when usd_goal<=19999 then '10000-19999' 
		 when usd_goal<=29999 then '20000-29999'
		 when usd_goal<=39999 then '30000-39999'
		 when usd_goal<=49999 then '40000-49999'
		 when usd_goal<=59999 then '50000-59999'
		 when usd_goal<=69999 then '60000-69999'
		 when usd_goal<=79999 then '70000-79999'
		  when usd_goal<=89999 then '80000-89999'
		  when usd_goal<=100000 then '90000-100000'
		  else '>100000' end as 'Goal_range'
		from projects
        where state='successful'),
cte1 as(select goal_range,count(goal_range) as 'Goal_range_count',Total_count
		from cte
		group by goal_range)
select *,(goal_range_count/total_count)*100 as '% of Success Rate' from cte1;
	
#-----------------------------------------Top 2 successful project names category wise based on backers_count-------------------------------------------------#
with cte as(select name,backers_count,c.country as 'Country' ,c.code as 'code',dense_rank() over(partition by  c.country order by backers_count desc) as 'dn'
			from projects p inner join country c on p.Country=c.code
            where state='successful')
select name , country, code ,backers_count as 'Backers_count' from cte where dn<=2;
            
#------------------------------------------Top 2 successful project names location wise based on backers_count-------------------------------------------------#

with cte as(select name,backers_count,c.country as 'Country' ,c.code as 'code',dense_rank() over(partition by  c.country order by backers_count desc) as 'dn'
			from projects p inner join country c on p.Country=c.code
            where state='successful')
select name , country, code ,backers_count as 'Backers_count' from cte where dn<=2;

#-----------------------------------------Top 2 successful project names category wise based on usd_pledged-------------------------------------------------#
with cte as(select p.name,p.usd_pledged,c.parent_category_name as 'category_name', dense_rank() over(partition by c.parent_category_name order by usd_pledged desc) as 'dn'
		from categorys c right join projects p on p.category_id=c.id
		where state='successful' and c.name is not null)
select name , category_name,usd_pledged as 'Amount raised' from cte where dn<=2;
            
#------------------------------------------Top 2 successful project names location wise based on usd_pledged-------------------------------------------------#

with cte as(select name,usd_pledged,c.country as 'Country' ,c.code as 'code',dense_rank() over(partition by  c.country order by usd_pledged desc) as 'dn'
			from projects p inner join country c on p.Country=c.code
            where state='successful')
select name , country, code ,usd_pledged as 'Amount raised' from cte where dn<=2;

#----------------------Highest amount raised for successful projects based on amount raised with category,loaction--------------------------------------#

with cte as(select usd_pledged,c.parent_category_name as 'category_name',a.country as 'Country' ,a.code as 'code', dense_rank() over(order by usd_pledged desc) as 'dn'
		from categorys c right join projects p on p.category_id=c.id left join country a on p.country=a.code
		where state='successful')
select category_name,country,code,usd_pledged as 'Amount raised' from cte where dn<=1;

#----------------------Highest amount raised for successful projects based on backers count with category,loaction--------------------------------------#

with cte as(select backers_count,c.parent_category_name as 'category_name',a.country as 'Country' ,a.code as 'code', dense_rank() over(order by backers_count desc) as 'dn'
		from categorys c right join projects p on p.category_id=c.id left join country a on p.country=a.code
		where state='successful')
select category_name,country,code,backers_count as 'Backers count' from cte where dn<=1;
