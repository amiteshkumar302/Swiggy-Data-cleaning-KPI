use Swiggy
select count(*) from swiggy_data

--DATA CLEANING & VALIDATION
--1.null value CHECK
 select 
  sum(case when State IS NULL Then 1 ELSE 0 END) AS null_state,
  sum(case when City IS NULL Then 1 ELSE 0 END) AS null_city,
  sum(case when Order_Date is null then 1 else 0 end) as null_endDate,
  sum(case when Restaurant_Name is null then 1 else 0 end) as null_restaurant,
  sum(case when Location is null then 1 else 0 end) as null_location,
  sum(case when Category is null then 1 else 0 end) as null_category,
  sum(case when Dish_Name is null then 1 else 0 end)as null_dish,
  sum(case when Rating is null then 1 else 0 end) as null_rating,
  sum(case when Rating_Count is null then 1 else 0 end) as null_ratingcount 
FROM swiggy_data

--2. BLANK OR ENPTY STRING
select * from swiggy_data where State='' or City='' or Location='' OR Rating='' or
Dish_Name='' or Rating_Count='';

--3. Duplicate Detection
select State,City, Order_Date, Restaurant_Name,Location, Category,rating, count(*) as CNT
from swiggy_data
GROUP BY
State,City, Order_Date, Restaurant_name,Location, Category,rating
HAVING COUNT(*)>1

--Delete Duplicate
with CTE as(
select *, ROW_NUMBER() Over(
Partition BY State,City, Order_Date, Restaurant_Name,Location, Category,rating
ORDER BY(SELECT NULL)
) AS rn
from swiggy_data
)
DELETE FROM CTE WHERE rn>1

--create schema
--dimenson table
--date table
create table dim_date(
date_id int identity(1,1) primary key,
full_date date,
Year int,
Month_Name varchar(20),
Quarter int,
Day int,
Week int
)

--dim location
create table dim_location(
location_id int identity(1,1) primary key,
state varchar(100),
city varchar(100),
location varchar(200)
)

--dim restaurant
create table dim_restaurant(
restaurant_id int identity(1,1) primary key,
Restaurant_Name varchar(200)
)

--dim category
create table dim_category(
category_id int identity(1,1) primary key,
Category varchar(200)
)

--dim dish
create table dim_dish(
dish_id int identity(1,1) primary key,
Dish_name varchar(200)
)


--create fact table
create table fact_swiggy_orders(
order_id int identity(1,1) primary key,
date_id int,
price_INR decimal(4,2),
rating_count int,

location_id int,
restaurant_id int,
category_id int,
dish_id int,

foreign key (date_id) references dim_date(date_id),
foreign key (location_id) references dim_location(location_id),
foreign key (restaurant_id) references dim_restaurant(restaurant_id),
foreign key (category_id) references dim_category(category_id),
foreign key (dish_id) references dim_dish(dish_id)
);

--INSERT DATA IN TABLES
--dim data
INSERT INTO dim_date(full_date,year,Month,Month_Name,Quarter,day,week)
select distinct
   Order_date,
   YEAR(order_date),
   Month(order_date),
   DATENAME(MONTH,ORDER_DATE),
   DATEPART(QUARTER,ORDER_DATE),
   DAY(ORDER_DATE),
   DATEPART(WEEK,ORDER_DATE)
FROM swiggy_data
where Order_Date is not null;

select * from dim_date

--dim location
insert into dim_location(state,city,Location)
select distinct state,city,location
from swiggy_data;

select * from dim_location

--dim restaurant
insert into dim_restaurant(Restaurant_Name)
select distinct
 restaurant_name
 from swiggy_data

 --dim category
insert into dim_category(Category)
select distinct
 category
 from swiggy_data

 --dim dish
insert into dim_dish(Dish_name)
select distinct
 Dish_Name
 from swiggy_data

 --insert into fact table
 insert into fact_swiggy_orders
 (
   date_id,
   price_INR,
   rating,
   rating_count,
   location_id,
   restaurant_id,
   category_id,
   dish_id
 )
 select 
   dd.date_id,
   s.Price_INR,
   s.rating,
   s.rating_count,
   dl.location_id,
   dr.restaurant_id,
   dc.category_id,
   dsh.dish_id
   from swiggy_data s

   join dim_date dd
   on dd.full_date = s.Order_Date

   join dim_location dl
   on dl.state=s.State
   and dl.city=s.City
   and dl.location=s.Location

   join dim_restaurant dr
   on dr.Restaurant_Name=s.Restaurant_Name

   join dim_category dc
   on dc.Category=s.Category

   join dim_dish dsh
   on dsh.Dish_name=s.Dish_Name;

--KPI's
--Total Orders
select count(*) as total_orders from fact_swiggy_orders

--Total Revenue
select FORMAT(SUM(CONVERT(float,Price_INR))/1000000,'N2')+'INR Million' AS Total_Revenue from fact_swiggy_orders 

--Average dish price
select FORMAT(Avg(CONVERT(float,Price_INR)),'N2')+'INR ' AS Average_dish_price from fact_swiggy_orders 

--Average Rating
select AVG(Rating) as Avg_Rating from fact_swiggy_orders

--Deep dive business analysis
--monthly order trends																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																													
select d.year,d.month,d.month_name,																																																																																																																																																																																									
count(*) AS Total_Orders from fact_swiggy_orders f
join dim_date d	on f.date_id=d.date_id	
Group by d.year,d.month,d.month_name

--Quarterly order trends
select d.year,d.Quarter,																																																																																																																																																																																									
count(*) AS Total_Orders from fact_swiggy_orders f
join dim_date d	on f.date_id=d.date_id	
Group by d.year,d.Quarter

--yearly trends
select d.year,																																																																																																																																																																																									
count(*) AS Total_Orders from fact_swiggy_orders f
join dim_date d	on f.date_id=d.date_id	
Group by d.year
order by count(*) desc

--order by day of week (mon-sat)
select DATENAME(WEEKDAY,D.full_date) AS day_name,																																																																																																																																																																																									
count(*) AS Total_Orders from fact_swiggy_orders f
join dim_date d	on f.date_id=d.date_id	
Group by DATENAME(WEEKDAY,d.full_date), DATEPART(WEEKDAY,d.full_date)
order by DATEPART(WEEKDAY,d.full_date)

--Top 10 cities by order volume
select TOP 10
l.city,
count(*) as total_orders from fact_swiggy_orders f
join dim_location l
on l.location_id=f.location_id
group by l.city
order by count(*) desc

--Revenue contribution by state
select 
l.state,
sum(f.price_INR) as total_revenue from fact_swiggy_orders f
join dim_location l
on l.location_id=f.location_id
group by l.state
order by sum(f.price_INR) desc

--Top 10 restaurant by orders
select Top 10
r.restaurant_name,
sum(f.price_INR) as total_revenue from fact_swiggy_orders f
join dim_restaurant r
on r.restaurant_id=f.restaurant_id
group by r.restaurant_name
order by sum(f.price_INR) desc

--Top category by order volume
select
c.category,
count(*) as total_orders from fact_swiggy_orders f
join dim_category c
on c.category_id=f.category_id
group by c.category
order by total_orders desc

--Most order dish
select
d.Dish_Name,
count(*) as order_count from fact_swiggy_orders f
join dim_dish d
on d.Dish_id=f.dish_id
group by d.Dish_Name
order by order_count desc

--Total orders by price range
select
 CASE 
 WHEN CONVERT(FLOAT,price_INR)<100 THEN 'UNDER 100'
 WHEN CONVERT(FLOAT,price_INR) BETWEEN 100 AND 199 THEN '100-199'
  WHEN CONVERT(FLOAT,price_INR) BETWEEN 200 AND 299 THEN '200-299'
  WHEN CONVERT(FLOAT,price_INR) BETWEEN 300 AND 499 THEN '300-499'
 ELSE '500+'
 END AS price_range,
 count(*) as total_orders
 from fact_swiggy_orders
 group by
 CASE 
 WHEN CONVERT(FLOAT,price_INR)<100 THEN 'UNDER 100'
 WHEN CONVERT(FLOAT,price_INR) BETWEEN 100 AND 199 THEN '100-199'
  WHEN CONVERT(FLOAT,price_INR) BETWEEN 200 AND 299 THEN '200-299'
  WHEN CONVERT(FLOAT,price_INR) BETWEEN 300 AND 499 THEN '300-499'
 ELSE '500+'
 END
 ORDER BY total_orders DESC

 --TOTAL RATING COUNT DISTRIBUTION (1-5)
 SELECT RATING,COUNT(*) As Rating_Count
 FrOM fact_swiggy_orders
 Group By Rating
 Order By Rating DESC