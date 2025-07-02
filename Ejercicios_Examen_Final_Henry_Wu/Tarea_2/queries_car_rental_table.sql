--queries del ejercicio numero 2. Examen 1


--query 1
select count(cra.fueltype) as total_rent_number from car_rental_analytics cra
where cra.fueltype  in ('electric','hybrid') and cra.rate_daily >=4

--query 2
select cra.state_name,count(cra.state_name) as rent_quantity from car_rental_analytics cra
group by cra.state_name order by rent_quantity asc limit 5

--query 3
select cra.model,cra.make,count(cra.model) as total_rent_number from car_rental_analytics cra
group by cra.model,cra.make order by total_rent_number desc limit 10

--query 4
select cra.`year` as make_year,count(cra.`year`) as total_rent_number from car_rental_analytics cra
where cra.`year` between 2010 and 2015
group by cra.`year` order by cra.`year` desc

--query 5
select cra.city,count(cra.city) as total_rent_number from car_rental_analytics cra
where cra.fueltype in ('electric','hybrid') group by cra.city order by total_rent_number desc limit 5

--query 6
select cra.fueltype,avg(cra.rating) as avg_rating from car_rental_analytics cra where cra.fueltype is not null
group by cra.fueltype order by avg_rating desc