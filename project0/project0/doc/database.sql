-- Users table
create table users (
	user_id SERIAL primary key,
	first_name varchar(32) not null,
	last_name varchar(32) not null,
	age integer not null,
	username varchar(16) not null,
	is_admin boolean not null default false,
	password text not null
);

create table debit_acc (
	acc_id SERIAL primary key,
	-- A foreign key constraint that is linked to the users table
	-- The foreign key has additional constraint when a user is deleted, the row that is connected to that user_id will also
	-- be deleted
	user_id INTEGER not null references users(user_id) on delete cascade,
	balance decimal not null default 0
);

create table credit_acc (
	acc_id SERIAL primary key,
	-- A foreign key constraint that is linked to the users table
	-- The foreign key has additional constraint when a user is deleted, the row that is connected to that user_id will also
	-- be deleted
	user_id INTEGER not null references users(user_id) on delete cascade,
	balance decimal not null default 0,
	cash_awards decimal not null default 0
);


-- A SQL Function that handles creating accounts of users when they register to the bank app
-- When a user registers a corresponding debit and credit account will be created for them
-- It is not how it happens in real life but its fun to have it.
CREATE OR REPLACE FUNCTION create_accs() RETURNS TRIGGER AS
$BODY$
BEGIN
	INSERT INTO debit_acc(user_id, balance) VALUES (NEW.user_id, 50);
	INSERT INTO credit_acc(user_id) VALUES (NEW.user_id);
	RETURN NEW;
END;
$BODY$
language plpgsql;

-- A trigger that calls the function above when a insert query is executed.
CREATE TRIGGER on_insert_user AFTER INSERT ON users
	FOR EACH ROW
	EXECUTE procedure create_accs();

drop table users cascade;
drop table debit_acc;
drop table credit_acc;

ALTER TABLE users ADD CONSTRAINT username_unique UNIQUE (username);

-- Truncates the users table and will also delete the rows that are connected from debit and credit acc tables.
truncate users cascade;

-- Random CRUD operations testing for application.
SELECT * from users;
SELECT * from debit_acc;
SELECT * from credit_acc;

insert into users(first_name, last_name, age, username, password) values
('Jefferson', 'Santiago', 23, 'jeff', 'password123');

SELECT user_id, username, is_admin FROM users WHERE is_admin = false;

DELETE FROM users WHERE user_id = 5;

SELECT user_id, username, is_admin FROM users WHERE username = 'jeff' AND password = 'password123' LIMIT 1

UPDATE debit_acc SET balance = balance + 50 WHERE user_id = 2;

SELECT first_name, last_name, d.acc_id AS debit_num, d.balance AS debit_bal, c.acc_id AS credit_num, c.balance AS credit_bal, cash_awards AS credit_awards FROM users u 
	INNER JOIN debit_acc d ON d.user_id = u.user_id 
	INNER JOIN credit_acc c ON c.user_id = u.user_id;
	
