import numpy as np
from sklearn.linear_model import LinearRegression


def flatten(ll):
    '''
    Flattens a list of lists into a single list
    '''
    return [item for sublist in ll for item in sublist]


def compute_cumulative(xs):
    '''
    Computes a running total (i.e. cumulative sum)
    for the given list.
    E.g. given [1, 2, 3, 4] the return value will be
    [1, 3, 6, 10].
    '''
    total = xs[0]
    result = [total]
    for x in xs[1:]:
        total += x
        result.append(total)
    return result


def linear_regression(xs, ys):
    '''
    Fits a line `y = mx + b` to the given data points
    '''
    x = np.array(xs).reshape((-1, 1))
    y = np.array(ys)
    model = LinearRegression().fit(x, y)
    return {
        'slope': model.coef_[0],
        'intercept': model.intercept_,
        'R^2': model.score(x, y),
    }


def compute_rate(timestamps):
    '''
    Given a list of timestamps indicating the times
    some event occured, returns the average rate at
    which the events happen. If the units of the
    timestamps are seconds, then the output units will
    be `events/s`.
    '''
    cumulative_events = [i for i in range(1, len(timestamps) + 1)]
    fit = linear_regression(timestamps, cumulative_events)
    return fit['slope']
