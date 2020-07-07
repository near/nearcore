from prometheus_client import parser
import requests
import time


def fold(collection, key, f, default):
    if key in collection:
        return f(collection[key])
    else:
        return default


class Metrics:

    def __init__(self, total_blocks, memory_usage, total_transactions,
                 block_processing_time, timestamp):
        self.total_blocks = total_blocks
        self.memory_usage = memory_usage
        self.total_transactions = total_transactions
        self.block_processing_time = block_processing_time
        self.timestamp = timestamp

    @classmethod
    def from_url(cls, metrics_url):
        response = requests.get(metrics_url, timeout=10)
        timestamp = time.time()
        response.raise_for_status()
        prometheus_string = response.content.decode('utf8')
        prometheus_metrics = dict(
            map(lambda m: (m.name, m),
                parser.text_string_to_metric_families(prometheus_string)))

        fold_sample = lambda key: fold(prometheus_metrics, key, lambda m: int(
            m.samples[0].value), 0)

        total_blocks = fold_sample('near_block_processed')
        memory_usage = fold_sample('near_memory_usage_bytes')
        total_transactions = fold_sample('near_transaction_processed')

        block_processing_time_samples = prometheus_metrics[
            'near_block_processing_time'].samples

        block_processing_time = {}
        for sample in block_processing_time_samples:
            if 'le' in sample.labels:
                bound = sample.labels['le']
                block_processing_time[f'le {bound}'] = int(sample.value)

        return cls(total_blocks, memory_usage, total_transactions,
                   block_processing_time, timestamp)

    @classmethod
    def diff(cls, final_metrics, initial_metrics):
        total_blocks = final_metrics.total_blocks - initial_metrics.total_blocks
        memory_usage = final_metrics.memory_usage - initial_metrics.memory_usage
        total_transactions = final_metrics.total_transactions - initial_metrics.total_transactions
        timestamp = final_metrics.timestamp - initial_metrics.timestamp
        block_processing_time = {}
        for sample in final_metrics.block_processing_time.keys():
            block_processing_time[sample] = final_metrics.block_processing_time[
                sample] - initial_metrics.block_processing_time[sample]

        return cls(total_blocks, memory_usage, total_transactions,
                   block_processing_time, timestamp)
