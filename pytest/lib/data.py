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


def flatten_dict(d):
    '''
    Flattens nested dictionaries to a single level. New keys have the form
    `outer_key.inner_key`. E.g. `{ 'a' : 0, 'b' : { 'x' : 'Hello', 'y' : 'World' } }`
    would become `{  'a' : 0, 'b.x' : 'Hello', 'b.y' : 'World' }`.
    '''
    dict_keys = [k for k in d.keys() if type(d[k]) == dict]
    if len(dict_keys) == 0:
        return d
    else:
        flattened_elements = [flatten_dict(d[k]) for k in dict_keys]
        scalar_keys = [k for k in d.keys() if k not in dict_keys]
        result = {}
        for k in scalar_keys:
            result[k] = d[k]
        for (k, sub_dict) in zip(dict_keys, flattened_elements):
            for sub_k in sub_dict:
                new_key = '.'.join([k, sub_k])
                result[new_key] = sub_dict[sub_k]
        return result


def dict_to_csv(ds, filename, mode='w'):
    '''
    Serializes a list of dictionaries to a csv file. Each element of the list is
    assumed to correspond to a single row. If the dictionary contains nested
    dictionaries, they are flattened in the output. Elements of the flattened
    dictionary must be scalar quantities (e.g. numbers and strings), not lists
    or tuples.
    '''
    keys = sorted(list(flatten_dict(ds[0]).keys()))
    header = ','.join(keys)
    with open(filename, mode) as output:
        output.write(header + '\n')
        for d in ds:
            f = flatten_dict(d)
            row = ','.join(map(lambda k: str(f[k]), keys))
            output.write(row + '\n')
