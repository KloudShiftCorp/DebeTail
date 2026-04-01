create database blog;

use blog;

create table Author
(
    Id        int primary key,
    FirstName varchar(255),
    LastName  varchar(255)
);

create table Post
(
    Id       int primary key,
    Title    varchar(255),
    Content  varchar(255),
    AuthorId int,

    constraint Blog_Author_Id_fk
        foreign key (AuthorId) references dbo.Author
);

-- enable cdc for a database
-- https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-data-capture-sql-server?view=sql-server-2016#enable-for-a-database
if not exists(select 1
              from sys.databases
              where name = DB_NAME()
                and is_cdc_enabled = 1)
    begin
        exec sys.sp_cdc_enable_db;
    end

select name, is_cdc_enabled
from sys.databases
where name = 'blog';

-- enable cdc for tables using sys.sp_cdc_enable_table
-- https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-data-capture-sql-server?view=sql-server-2016#enable-for-a-table
if not exists(select 1
              from cdc.change_tables
              where source_object_id = OBJECT_ID('dbo.Author'))
    begin
        exec sys.sp_cdc_enable_table
             @source_schema = 'dbo',
             @source_name = 'Author',
             @role_name = NULL,
             @supports_net_changes = 0;
    end

if not exists(select 1
              from cdc.change_tables
              where source_object_id = OBJECT_ID('dbo.Post'))
    begin
        exec sys.sp_cdc_enable_table
             @source_schema = 'dbo',
             @source_name = 'Post',
             @role_name = NULL,
             @supports_net_changes = 0;
    end

select * 
from sys.tables
where is_tracked_by_cdc = 1;
-- or alternatively    
select *
from cdc.change_tables;

SELECT
    ct.capture_instance,
    OBJECT_SCHEMA_NAME(ct.source_object_id) AS schema_name,
    OBJECT_NAME(ct.source_object_id)        AS table_name
FROM cdc.change_tables ct
ORDER BY schema_name, table_name, ct.capture_instance

-- cdc testing

-- Lowest LSN available in the capture instance
DECLARE @from_lsn BINARY(10) = sys.fn_cdc_get_min_lsn('dbo_Author');
-- Highest LSN currently in the log
DECLARE @to_lsn   BINARY(10) = sys.fn_cdc_get_max_lsn();
-- get changes
SELECT 
        *               -- your table columns follow
FROM    cdc.fn_cdc_get_all_changes_dbo_Author(
        @from_lsn,
        @to_lsn,
        N'all update old'      -- 'all' or 'all update old' (includes before-image rows)
        )
ORDER BY __$start_lsn, __$seqval;

-- if sys.fn_cdc_get_max_lsn() is NULL, then the capture instance is not enabled, check with.
select
    dss.servicename,
    dss.status,
    dss.status_desc
from sys.dm_server_services dss


insert into Author (Id, FirstName, LastName) values (1, 'John', 'Doe');

insert into Post (Id, Title, Content, AuthorId) values (1, 'Hello World', 'This is my first post', 1);

delete from Post;
delete from Author;