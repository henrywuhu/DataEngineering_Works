
--query numero 1
select count(at2.fecha) as total_vuelos from aeropuerto_tabla at2 where at2.fecha  between '2021-12-01' and '2022-01-31'

--query numero 2
select sum(at2.pasajeros) as total_vuelos_aerolinea_argentina from aeropuerto_tabla at2 where at2.fecha  between '2021-01-01' and '2022-06-30' and at2.aerolinea_nombre = 'AEROLINEAS ARGENTINAS SA'

--query numero 3
select at2.fecha,at2.horautc,at2.aeropuerto as aeropuerto_salida,q1.provincia as ciudad_salida,at2.pasajeros,at2.origen_destino as aeropuerto_arribo,q2.provincia as ciudad_arribo from aeropuerto_tabla at2 
join (select at2.aeropuerto,adt.provincia from aeropuerto_tabla at2 
join aeropuerto_detalles_tabla adt on at2.aeropuerto = adt.aeropuerto
group by at2.aeropuerto,adt.provincia) q1
on at2.aeropuerto  = q1.aeropuerto 
join (select at2.origen_destino ,adt.provincia from aeropuerto_tabla at2 
join aeropuerto_detalles_tabla adt on at2.origen_destino  = adt.aeropuerto
group by at2.origen_destino ,adt.provincia) q2
on at2.origen_destino  = q2.origen_destino
where at2.fecha between '2021-01-01' and '2022-06-30' and at2.tipo_de_movimiento = 'Despegue' and at2.pasajeros>0
order by at2.fecha desc

--query numero 4
select at2.aerolinea_nombre,sum(at2.pasajeros) as numero_pasejeros from aeropuerto_tabla at2 where at2.aerolinea_nombre is not NULl and at2.aerolinea_nombre not in ('0')  and at2.fecha between '2021-01-01' and '2022-06-30'
group by at2.aerolinea_nombre order by numero_pasejeros  desc LIMIT 10

--query numero 5
select at2.aeronave, count(at2.aeronave) as numero_vuelos from aeropuerto_tabla at2 
join aeropuerto_detalles_tabla adt on at2.aeropuerto  = adt.aeropuerto
where at2.fecha between '2021-01-01' and '2022-06-30' and adt.provincia in ('BUENOS AIRES','CIUDAD AUTÓNOMA DE BUENOS AIRES') and at2.tipo_de_movimiento = 'Despegue' and at2.aeronave not in ('0')
GROUP BY at2.aeronave order by numero_vuelos  desc LIMIT 10