# rust_pg_extensions

```
cargo install --locked cargo-pgrx
cargo pgrx init
cargo pgrx run pg13
```


```
drop extension if exists all_in_one_lib cascade;
create extension all_in_one_lib;
```

data_encrypt & data_decrypt example.

```
all_in_one_lib=# SELECT data_encrypt('01234567890123456789012345678901', 'hello world!!');
                         data_encrypt                         
--------------------------------------------------------------
 \x9cb017fde9c75f5d884e42253c0784ec0935106550e3735f662e0c0756
(1 row)

all_in_one_lib=# SELECT data_decrypt('01234567890123456789012345678901','\x9cb017fde9c75f5d884e42253c0784ec0935106550e3735f662e0c0756');
 data_decrypt  
---------------
 hello world!!
(1 row)
```

## FDW

```
create foreign data wrapper default_wrapper
  handler default_fdw_handler;
  
create server my_default_server
  foreign data wrapper default_wrapper
  options (
    foo 'bar'
  );

create foreign table hello (
  id bigint,
  col text
)
server my_default_server options (
	foo 'bar'
);
```