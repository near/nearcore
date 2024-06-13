-- Assumes regularly values handled here cannot be negative, hence -1 flags out
-- of range values.
create function convert_int_to_bigint(x bigint)
returns integer as $$
begin
    if x between -2147483648 and 2147483647 then
        return x;
    else
        return -1;
    end if;
end;
$$ language plpgsql;

alter table ft_transfers drop column time_end;
-- Ensure the bigint -> integer conversion will succeed.
update ft_transfers set
    size_state_bytes = convert_int_to_bigint(size_state_bytes),
    total_transactions = convert_int_to_bigint(total_transactions);
alter table ft_transfers
alter column size_state_bytes type integer,
alter column total_transactions type integer;

drop function convert_int_to_bigint (integer);
