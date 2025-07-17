select C.name,sum(O.amount) as total_amount from customers C INNER JOIN orders O on C.id = O.customer_id group by C.name;

select * from (select C.name,sum(O.amount) as total_amount from customers C INNER JOIN orders O on C.id = O.customer_id group by C.name) D where total_amount > 10000;

select * from orders;

select * from (select C.name,sum(O.amount) as total_amount from customers C INNER JOIN orders O on C.id = O.customer_id group by amount) 