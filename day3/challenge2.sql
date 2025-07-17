use ecommerce;

--- task1

select c.id, (select o.amount from orders o where c.id=o.customer_id order by o.order_date desc limit 1) as recent_order_amount from customers c;

--- task2
select * from orders;

select count(*) as total_orders from orders;

select count(*) from orders where amount>5000;

select count(*) from orders where amount<1000;

--- task 3
select c.id,c.name, count(*) as total_orders from customers c inner join orders o on c.id=o.customer_id group by c.id;
