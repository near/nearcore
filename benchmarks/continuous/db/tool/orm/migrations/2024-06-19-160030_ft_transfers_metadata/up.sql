-- Setting default values to enable `not null` by filling values for existing
-- rows.
alter table ft_transfers
add column initiator text not null default 'crt-benchmarks',
add column context text not null default 'scheduled benchmark run';

-- Drop defaults to enforce new data to explicitly set these columns.
alter table ft_transfers
alter column initiator drop default,
alter column context drop default;
