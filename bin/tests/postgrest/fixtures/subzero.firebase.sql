drop schema if exists firebase cascade;
create schema firebase;
grant usage on schema firebase to public;

create or replace function firebase.new_id() returns text as $$
declare 
    chars text[] := string_to_array('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789',null);
    chars_length integer;
    maxMultiple integer;
    autoId text := '';
    targetLength integer := 20;
    bytes bytea;
    i integer := 0;
begin
    chars_length := array_length(chars,1);
    maxMultiple := floor(256 / chars_length) * chars_length;
    while length(autoId) < targetLength loop
        bytes := gen_random_bytes(40);
        while i < octet_length(bytes) loop
            if length(autoId) < targetLength and get_byte(bytes, i) < maxMultiple then
                autoId := concat(autoId,chars[ get_byte(bytes, i) % chars_length ]);
            end if;
            i := i + 1;
        end loop;
    end loop;
    return autoId;
end;
$$ language plpgsql;