create extension  all_in_one_lib ;

create foreign data wrapper default_wrapper handler default_fdw_handler;
  
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

insert into hello values (1,'test1');
insert into hello values (2,'test2');
insert into hello values (21,'test21');
insert into hello values (123,NULL);

SELECT * FROM hello;