create extension  all_in_one_lib ;

create foreign data wrapper csv_wrapper handler csv_fdw_handler;
  
create server csv_server foreign data wrapper csv_wrapper;

CREATE foreign TABLE users (
    id INT,
    name VARCHAR(100),
    email VARCHAR(100),
    age INT
) server csv_server options (
	 filepath 'replace_with_your_path/people_info.csv'
);


SELECT * FROM users;