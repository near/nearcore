grant select on ft_transfers to grafana_reader;
grant select on ft_transfers to benchmark_runner;
grant insert on ft_transfers to benchmark_runner;

revoke pg_read_all_data from grafana_reader;
revoke pg_read_all_data from benchmark_runner;
revoke pg_write_all_data from benchmark_runner;
