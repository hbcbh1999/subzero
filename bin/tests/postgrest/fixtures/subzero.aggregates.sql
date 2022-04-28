CREATE TABLE test.product_orders
(
	order_id INT,
	order_date DATE,
	customer_name VARCHAR(250),
	city VARCHAR(100),	
	order_amount MONEY
);

GRANT ALL ON TABLE test.product_orders TO postgrest_test_anonymous;

TRUNCATE TABLE test.product_orders CASCADE;
INSERT INTO test.product_orders VALUES
('1001','04/01/2017','David Smith','GuildFord',10000),
('1002','04/02/2017','David Jones','Arlington',20000),
('1003','04/03/2017','John Smith','Shalford',5000),
('1004','04/04/2017','Michael Smith','GuildFord',15000),
('1005','04/05/2017','David Williams','Shalford',7000),
('1006','04/06/2017','Paum Smith','GuildFord',25000),
('1007','04/10/2017','Andrew Smith','Arlington',15000),
('1008','04/11/2017','David Brown','Arlington',2000),
('1009','04/20/2017','Robert Smith','Shalford',1000),
('1010','04/25/2017','Peter Smith','GuildFord',500);


create or replace function validate(
  valid boolean, 
  err text,
  details text default '',
  hint text default '',
  errcode text default 'P0001'
) returns boolean as $$
begin
   if valid then
      return true;
   else
      RAISE EXCEPTION '%', err USING
      DETAIL = details, 
      HINT = hint, 
      ERRCODE = errcode;
   end if;
end
$$ stable language plpgsql;

create view test.protected_books as 
  select id, title, publication_year, author_id 
  from private.books 
  where 
    validate(
      --(current_setting('request.path', true) != '/rest/protected_books') or -- do not check for filters when the view is embeded
      ( -- check at least one filter is set
        -- this branch is activated only when request.path = /protected_books
        (current_setting('request.get.id', true) is not null) or
        (current_setting('request.get.publication_year', true) is not null) or
        (current_setting('request.get.author_id', true) is not null)
      ),
      'Filter parameters not provided',
      'Please provide at least one of id, publication_year, author_id filters'
    )

  ;
GRANT ALL ON TABLE test.protected_books TO postgrest_test_anonymous;