alter table ft_transfers add column time_end timestamp with time zone;
-- Fill value for existing rows to allow setting `not null` in the next step.
update ft_transfers set time_end = time;
alter table ft_transfers alter column time_end set not null;
alter table ft_transfers
alter column size_state_bytes type bigint,
alter column total_transactions type bigint;
