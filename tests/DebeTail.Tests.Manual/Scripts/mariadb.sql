create schema blog;
 
use blog;

create table authors
(
    id         int primary key auto_increment,
    first_name varchar(255),
    last_name  varchar(255)
);

create table posts
(
    id         int primary key auto_increment,
    title      varchar(255),
    content    text,
    author_id  int,

    foreign key (author_id) references authors(id)
);

show master status;
show variables like 'binlog_format';

insert into authors (first_name, last_name) values ('John', 'Doe');
insert into posts (title, content, author_id) values ('Hello World', 'This is my first post', 1);

delete from posts;
delete from authors;