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

CREATE OR REPLACE FUNCTION get_param(name text) RETURNS text AS $$
DECLARE
    json_arr json;
    element json;
    i integer;
    param_name text;
    param_value text;
BEGIN
    json_arr := current_setting('request.get')::json;
    i := 0;
    WHILE i < json_array_length(json_arr) LOOP
        element := json_arr -> i;
        param_name := element ->> 0;
        param_value := element ->> 1;
        IF param_name = name THEN
            RETURN param_value;
        END IF;
        i := i + 1;
    END LOOP;
    RETURN NULL;  -- Returns NULL if the parameter is not found
END;
$$ LANGUAGE plpgsql;


create or replace function get_param_common(name text) returns text as $$
declare
  val text;
begin
  val :=  case when current_setting('server_version_num')::int >= 140000
          then get_param(name)
          else current_setting('request.get.'||name,true)::text
          end;
  return val;
end
$$ stable language plpgsql;


create or replace function param_is_set(name text) returns boolean as $$
declare
  val text;
begin
  val := get_param_common(name);
  return val is not null and val != '';
end
$$ stable language plpgsql;

create view test.protected_books as 
  select id, title, publication_year, author_id,
  get_param_common('author_id') as v_author_id,
  get_param_common('id') as v_id,
  get_param_common('publication_year') as v_publication_year
  from private.books 
  where 
    validate(
      --(current_setting('request.path', true) != '/rest/protected_books') or -- do not check for filters when the view is embeded
      ( -- check at least one filter is set
        -- this branch is activated only when request.path = /protected_books
        param_is_set('author_id') or
        param_is_set('id') or
        param_is_set('publication_year')
        
      ),
      'Filter parameters not provided',
      'Please provide at least one of id, publication_year, author_id filters'
    )

  ;
GRANT ALL ON TABLE test.protected_books TO postgrest_test_anonymous;