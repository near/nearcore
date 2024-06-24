-- Cleanup permissions granted previously. See comment below for motivation.
revoke select on ft_transfers from grafana_reader;
revoke select on ft_transfers from benchmark_runner;
revoke insert on ft_transfers from benchmark_runner;

-- Granting individual permissions like done previously is tedious and error
-- prone. In addition it must be repeated for all new tables.
-- The benchmarks db contains no sensitive data, so we can use
-- `pg_read_all_data` and `pg_write_all_data` to simplify things. These
-- permissions automatically apply to new tables added in the future.
grant pg_read_all_data to grafana_reader;
grant pg_read_all_data to benchmark_runner;
grant pg_write_all_data to benchmark_runner;
