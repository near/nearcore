import collections
from mocknet.helper import get_metrics
from configured_logger import logger

METRICS_MAPPING = {
    "height": "near_block_height_head",
    "epoch_height": "near_epoch_height",
    "expected_chunks": 'near_validators_chunks_expected',
    "produced_chunks": 'near_validators_chunks_produced',
    "sync_status": "near_sync_status",
}
RED_COLOR = '\033[31m'
GREEN_COLOR = '\033[32m'
YELLOW_COLOR = '\033[33m'
BOLD = '\033[1m'
END_FORMAT = '\033[0m'


class NodeMetrics:

    def __init__(self, nodes):
        self.metrics = {node["name"]: get_metrics(node) for node in nodes}
        # for every node calculating missed chunks of connected account (if any exists)
        for node, metrics in self.metrics.items():
            for full_label_str in metrics["expected_chunks"].keys():
                account_id = full_label_str.strip()
                if account_id in node:
                    expected_chunks = self.get_metric(node, "expected_chunks",
                                                      full_label_str)
                    produced_chunks = self.get_metric(node, "produced_chunks",
                                                      full_label_str)
                    metrics['missed_chunks'][str(
                        set())] = expected_chunks - produced_chunks

    def get_metric(self, node_name, metric_name, labels=set()):
        if metric_name in METRICS_MAPPING:
            return self.metrics[node_name][METRICS_MAPPING[metric_name]].get(
                str(labels))
        else:
            return self.metrics[node_name][metric_name].get(str(labels))


# Get the color code for this value based on provided bounds dict.
# If value is None, yellow color is returned.
# If value is out of provided bounds, red color is returned.
# If value is in the bounds (for example, if no bounds are provided), green color is returned.
def stat_color(value, bounds):
    min_value = bounds.get('min')
    max_value = bounds.get('max')
    if value is None:
        return YELLOW_COLOR
    elif min_value is not None and value < min_value:
        return RED_COLOR
    elif max_value is not None and value > max_value:
        return RED_COLOR
    else:
        return GREEN_COLOR


class NodeStats:

    def __init__(self, first_metrics, second_metrics, time_elapsed):
        self.stats = collections.defaultdict(dict)
        for node, _ in second_metrics.metrics.items():
            if node not in first_metrics.metrics:
                continue

            def set_diff(metric_name, stat_name):
                metric2 = second_metrics.get_metric(node, metric_name)
                metric1 = first_metrics.get_metric(node, metric_name)
                if metric2 is not None and metric1 is not None:
                    self.stats[node][stat_name] = (metric2 -
                                                   metric1) / time_elapsed
                else:
                    self.stats[node][stat_name] = None

            set_diff("height", "bps")
            set_diff("missed_chunks", "missed_chunks")

            self.stats[node]["sync_status"] = second_metrics.get_metric(
                node, "sync_status")
            self.stats[node]["epoch_height"] = second_metrics.get_metric(
                node, "epoch_height")

    # stats_to_output -- what stats to output, what value is considered max/min ok for this stat.
    # This is needed to decide the color of the stats. If no bounds are provided, green color will be chosen.
    # For more details look at stat_color function.
    def print_stat_report(self, stats_to_output):
        logger.info("\n".join(["NodeStats:"] + list(
            map(
                lambda it: '\t'.join([f"{it[0]}"] + [
                    f"{stat_color(it[1].get(stat_name), bounds)}{stat_name}:{it[1].get(stat_name)}{END_FORMAT}"
                    for (stat_name, bounds) in stats_to_output.items()
                ]), self.stats.items()))))


class NodeAccumulationStats:

    def __init__(self, all_stats):
        self.raw_stats = collections.defaultdict(
            lambda: collections.defaultdict(list))
        for stats in all_stats:
            for node_name, node_stats in stats.stats.items():
                for stat_name, stat_value in node_stats.items():
                    self.raw_stats[node_name][stat_name].append(stat_value)
        self.stats = collections.defaultdict(
            lambda: collections.defaultdict(dict))
        for node_name, raw_stats in self.raw_stats.items():
            for stat_name, data in raw_stats.items():
                filtered = list(filter(lambda x: x is not None, data))
                self.stats[node_name][stat_name]['min'] = None if len(
                    filtered) == 0 else min(filtered)
                self.stats[node_name][stat_name]['max'] = None if len(
                    filtered) == 0 else max(filtered)
                self.stats[node_name][stat_name]['avg'] = None if len(
                    filtered) == 0 else sum(filtered) / len(filtered)

    def print_report(self, stats_to_output):
        logger.info("NodeAccumulationStats:")
        for stat_name, stat_bounds in stats_to_output.items():
            logger.info(f"{BOLD}{stat_name}{END_FORMAT}")
            for node_name, node_stats in self.stats.items():
                line = [node_name]
                if stat_name in node_stats:
                    for acc in ['min', 'max', 'avg']:
                        value = node_stats[stat_name][acc]
                        line.append(
                            f"{stat_color(value, stat_bounds)}{value}{END_FORMAT}"
                        )
                logger.info('\t'.join(line))
