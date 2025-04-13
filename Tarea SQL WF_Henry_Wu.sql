



--- TRABAJO SQL WF BOOTCAMP DATAENGINEER.

-- Query 1. AVG
select c.category_name, p.product_name,p.unit_price, avg(p.unit_price) over (partition by p.category_id) as "avgpricebycategory"
from products p inner join categories c on p.category_id  = c.category_id;


--Query 2 AVG
-- Creando una tabla temporal que contenga el total del pedido

create temp table temp_total_order as
select q.order_id, sum(q.order_amount) as "total_amount" from 
(select od.order_id,od.product_id ,(od.unit_price * od.quantity * (1-od.discount)) as "order_amount" from order_details od) as q
group by q.order_id;

-- Una vez creada la tabla temporal hacer el query con el SQL WF
select avg(tt.total_amount) over (partition by o.customer_id) as "avgorderamount",* from orders o
inner join temp_total_order tt on tt.order_id = o.order_id;


--Query 3 AVG

select p.product_name,c.category_name,p.quantity_per_unit,p.unit_price,od.quantity,
avg(od.quantity) over (partition by c.category_name) as "avgquantity"
from order_details od 
inner join products p on p.product_id  = od.product_id
inner join categories c  on c.category_id  = p.category_id;

--Query 4 Min
select o.customer_id,o.order_date,min(o.order_date) over (partition by o.customer_id) as "earliestorderdate" from orders o;

--Query 5 Max
select p.product_id,p.product_name,p.unit_price,p.category_id,max(p.unit_price) over (partition by p.category_id) as "maxunitprice" from products p;

--Query 6 Row_number
select row_number() over (order by  sum(od.quantity) desc) as "ranking",p.product_name,sum(od.quantity) as "totalquantity" from order_details od
inner join products p  on p.product_id  = od.product_id
group by p.product_name;

--Query 7 Row_number

SELECT 
    ROW_NUMBER() OVER (ORDER BY c.customer_id ASC) AS "rownumber",c.customer_id,c.company_name,c.contact_name,c.contact_title 
FROM 
    customers c;

--Query 8 Ranking
select rank() over (order by e.birth_date desc) as "ranking", concat(e.first_name,' ',e.last_name) as "employename",e.birth_date from employees e;

--Query 9 SUM
select sum(q.total_order) over (partition by o.customer_id) as "sumorderamount",o.order_id,o.customer_id,o.employee_id,o.order_date,o.required_date from orders o
inner join (
select od.order_id, (od.unit_price * od.quantity *(1-od.discount)) as "total_order" from order_details od) as q
on q.order_id  = o.order_id;

--Query 10 SUM
select c.category_name,p.product_name,p.unit_price,q.quantity,sum(q.total_order) over (partition by c.category_name) as "totalsales" from products p
inner join categories c on c.category_id = p.product_id
inner join(select od.product_id,od.quantity , (od.unit_price * od.quantity *(1-od.discount)) as "total_order" from order_details od) as q
on q.product_id  = p.product_id;

--Query 11 SUM
select o.ship_country,o.order_id,o.shipped_date,o.freight,sum(o.freight) over (partition by o.ship_country) as "totalshippingcosts" from orders o;

--Query 12 Rank
select o.customer_id,c.company_name,sum(q.sales) as "Total Sales", rank() over (order by sum(q.sales) desc) as "rank" from orders o
inner join customers c  on o.customer_id  = c.customer_id
inner join(
select od.order_id,sum(od.unit_price * od.quantity * (1-od.discount)) as "sales" from order_details od 
group by od.order_id) as q on q.order_id  = o.order_id
group by o.customer_id,c.company_name;


--Query 13 - Ranking fecha contratacion
select e.employee_id,e.first_name,e.last_name,e.hire_date, rank() over (order by e.hire_date asc) as "Rank" from employees e;

--Query 14 Ranking por productos
select p.product_id,p.product_name,p.unit_price, rank() over (order by p.unit_price desc) as "Rank" from products p;

--Query 15 LAG
select od.order_id,od.product_id,od.quantity,lag(od.quantity) over (partition by od.product_id) as "prevquantity" from order_details od 

--Query 16 LAG
select o.order_id,o.order_date,o.customer_id,lag(o.order_date) over (partition by o.customer_id order by o.order_date desc) from orders o;

--Query 17 Price difference
select *,(q.unit_price  - q.lastunitprice) as "pricedifference"  from(
select p.product_id,p.product_name,p.unit_price,lag(p.unit_price) over (order by p.product_id desc) as "lastunitprice" from products p) as q;

--Query 18 LEAD
select p.product_name,p.unit_price,lead(p.unit_price) over (order by p.product_name asc) as "nextprice" from products p;

--Query 19 LEAD
select *,lead(t.totalsales) over (order by t.category_name asc) as "nexttotalsales" from (
select s.category_name, sum(s.total) as "totalsales" from (
select c.category_name,q.total from categories c
inner join products p on p.category_id  = c.category_id
inner join (
select od.product_id ,sum((od.unit_price * od.quantity*(1-od.discount))) as "total" from order_details od group by od.product_id) as q on q.product_id = p.product_id) as s
group by s.category_name) as t;





