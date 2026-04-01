-- setup 
create table author
(
    id int primary key not null,
    firstname varchar(255),
    lastname varchar(255)
);

alter table author replica identity full;

create table post
(
    id int primary key not null,
    title varchar(255),
    content text,
    author_id int references author(id)
);

alter table author replica identity full;

-- display and verify replication slots
select * from pg_replication_slots; 

-- display and verify replication status
select * from pg_stat_replication;

-- display and verify publications
select * from pg_publication_tables;

-- testing data
insert into author (id, firstname, lastname) values (2, 'John', 'Doe');
insert into post (id, title, content, author_id) values (2, 'Hello World', 'This is my first post', 2);

delete from post;
delete from author;