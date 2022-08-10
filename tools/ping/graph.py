import sys
import numpy as np
import matplotlib.pyplot as plt
import argparse
import datetime
import dateutil


def filter_only_account_ids(latencies):
    a = [not x.startswith('ed25519:') for x in latencies['ID']]
    return latencies[a]


def plot_latencies(latencies, avg_latencies, num_timeouts, title):
    fig = plt.figure()
    if title is not None:
        fig.suptitle(title)
    ax = fig.add_subplot(1, 1, 1)
    ax.scatter(latencies['timestamp'], latencies['latency'], s=2)
    ax.plot(avg_latencies['timestamp'],
            avg_latencies['average latency'],
            color='red')
    ax.set_yscale('log')

    ax2 = ax.twinx()
    ax2.plot(num_timeouts['timestamp'],
             num_timeouts['num timeouts'],
             color='black')
    plt.show()


def extract_timeouts(filename):
    latencies = []
    timeouts = []

    with open(filename) as f:
        header_skipped = False
        while True:
            l = f.readline()
            if len(l) < 1:
                break
            if not header_skipped:
                header_skipped = True
                continue

            toks = l.strip().split(',')
            if len(toks) != 3:
                print(f'skipping bad line: {l}', file=sys.stderr)
                continue

            if toks[2] != 'TIMEOUT':
                latencies.append(
                    (np.datetime64(toks[0]), toks[1], int(toks[2])))
            else:
                timeouts.append((np.datetime64(toks[0]), toks[1]))

    latencies = np.array(latencies,
                         dtype=[('timestamp', 'datetime64[ms]'), ('ID', 'U64'),
                                ('latency', np.int64)])
    timeouts = np.array(timeouts,
                        dtype=[('timestamp', 'datetime64[ms]'), ('ID', 'U64')])
    return (latencies, timeouts)


def collect_window_avgs(latencies, window):
    avg_latencies = []

    current_window = None
    for l in latencies:
        timestamp = l['timestamp']
        latency = l['latency']
        if current_window is None:
            current_window = timestamp
            current_count = 1
            current_avg = latency
        elif timestamp - current_window > window:
            avg_latencies.append((current_window, current_avg))
            current_window = timestamp
            current_count = 1
            current_avg = latency
        else:
            current_avg = (
                (current_count) * current_avg + latency) / (current_count + 1)
            current_count += 1
    avg_latencies.append((current_window, current_avg))
    return np.array(avg_latencies,
                    dtype=[('timestamp', 'datetime64[ms]'),
                           ('average latency', np.int64)])


def collect_num_timeouts(timeouts, window):
    num_timeouts = []

    current_window = None

    for l in timeouts:
        timestamp = l['timestamp']
        if current_window is None:
            current_window = timestamp
            current_count = 1
        elif timestamp - current_window > window:
            num_timeouts.append((current_window, current_count))
            current_window = timestamp
            current_count = 1
        else:
            current_count += 1
    num_timeouts.append((current_window, current_count))
    return np.array(num_timeouts,
                    dtype=[('timestamp', 'datetime64[ms]'),
                           ('num timeouts', np.int32)])


def main(filename, window, title):
    latencies, timeouts = extract_timeouts(filename)
    latencies = filter_only_account_ids(latencies)

    avg_latencies = collect_window_avgs(latencies, window)
    num_timeouts = collect_num_timeouts(timeouts, window)
    plot_latencies(latencies, avg_latencies, num_timeouts, title)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='plot latencies')
    parser.add_argument('filename')
    parser.add_argument('--window-seconds', type=int, default=300)
    parser.add_argument('--title')
    args = parser.parse_args()

    window = np.timedelta64(args.window_seconds, 's')
    main(args.filename, window, args.title)
