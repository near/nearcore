import time
from configured_logger import logger
from mocknet.stats import NodeMetrics, NodeStats
from mocknet.helper import check_node_is_live


def loop_for(nodes, duration, all_stats, stats_bounds, break_condition=None):
    start_time = time.time()
    last_time = start_time
    last_metrics = NodeMetrics(nodes)
    while time.time() - start_time < duration:
        time.sleep(10)
        current_time = time.time()

        current_metrics = NodeMetrics(nodes)
        current_stats = NodeStats(last_metrics, current_metrics,
                                  current_time - last_time)
        current_stats.print_stat_report(stats_bounds)
        all_stats.append(current_stats)

        last_time = current_time
        last_metrics = current_metrics

        if break_condition is not None and break_condition(current_metrics):
            break

    return last_metrics


def wait_for_metric(node, metric_name):
    metrics = NodeMetrics([node])
    while metrics.get_metric(node['name'], metric_name) is None:
        time.sleep(30)
        metrics = NodeMetrics([node])
    return metrics, metrics.get_metric(node['name'], metric_name)


def wait_for_nodes_to_be_up(nodes):
    logger.info(
        f'Waiting for nodes {list(map(lambda x: x["name"], nodes))} to be up')
    for node in nodes:
        while not check_node_is_live(node):
            logger.info(f'Waiting for node {node["name"]} to be up')
            time.sleep(60)


def n_epochs_passed(current_metrics, node_name, n, start_epoch):
    current_epoch = current_metrics.get_metric(node_name, 'epoch_height')
    logger.info(f'Current epoch height for node {node_name} is {current_epoch}.'
                f'Waiting for {start_epoch + n}')
    return current_epoch is not None and current_epoch - start_epoch > n - 1


def wait_n_epochs(n, nodes, all_stats, stats_bounds, duration=float('inf')):
    # Wait for the first node to start and report epoch_height
    node_name = nodes[0]["name"]
    logger.info(f'Waiting for epoch height to apper in {node_name} metrics')
    _, start_epoch_height = wait_for_metric(nodes[0], 'epoch_height')

    logger.info(f'Waiting {n} epochs based on {node_name} metrics')
    return loop_for(nodes=nodes,
                    duration=duration,
                    all_stats=all_stats,
                    stats_bounds=stats_bounds,
                    break_condition=lambda x: n_epochs_passed(
                        x, node_name, n, start_epoch_height))


def nodes_are_in_sync(current_metrics, nodes):
    sync_statuses = [(node['name'],
                      current_metrics.get_metric(node['name'], 'sync_status'))
                     for node in nodes]
    sync_statuses = list(filter(lambda x: 0 < x[1], sync_statuses))
    logger.info(f'Waiting for nodes {sync_statuses} to sync')
    return len(sync_statuses) == 0


def wait_for_nodes_to_sync(sync_nodes,
                           all_nodes,
                           all_stats,
                           stats_bounds,
                           duration=float('inf')):
    logger.info(f'Waiting for nodes {sync_nodes} to sync')
    return loop_for(nodes=all_nodes,
                    duration=duration,
                    all_stats=all_stats,
                    stats_bounds=stats_bounds,
                    break_condition=lambda x: nodes_are_in_sync(x, sync_nodes))


# Clears data directories.
# Amends genesis.
def init_nodes(regular_nodes, traffic_nodes):
    # TODO(posvyatokum): implement
    pass
