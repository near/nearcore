revoke select on ft_transfers from grafana_reader;
revoke select on ft_transfers from benchmark_runner;
revoke insert on ft_transfers from benchmark_runner;
drop table ft_transfers;
