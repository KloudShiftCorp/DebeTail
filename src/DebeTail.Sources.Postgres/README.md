# Notes

## Enable logical replication for postgres

```sql
-- Test setup
create table blogs
(
    id    serial primary key,
    title text
);
alter table blogs replica identity full;

create table authors
(
    authors_id serial primary key,
    name       text
);
alter table authors replica identity full;

-- Create replication slot: pg_create_logical_replication_slot(slot_name name, plugin name [, temporary boolean])
select pg_create_logical_replication_slot('slotx', 'pgoutput', false);

-- Display & verify replication slots
select * from pg_replication_slots;

-- Create publication 
create publication pub1 for table blogs, authors;

-- Display & verify publications 
select * from pg_publication_tables;

-- Insert and trigger publish
insert into blogs values (default, 'test');
```

## Disable logical replication 

```sql
-- Remove replication slot
select pg_drop_replication_slot('slotx');

-- Remove publication
drop publication pub1;
```

## Message types

**INSERT**
- BeginMessage
- RelationMessage
- InsertMessage
- CommitMessage

**DELETE**
- BeginMessage
- RelationMessage
- FullDeleteMessage
- CommitMessage
