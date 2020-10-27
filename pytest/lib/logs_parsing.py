import datetime, time

# Date format in our logs (prepended with year, assumed to be this year)
DATE_FORMAT_STR = '%Y %b %d %H:%M:%S.%f'
YEAR = str(datetime.datetime.fromtimestamp(time.time()).year)


def remove_colour_codes(s):
    '''
    Consumes a string and srips out colour codes from it.
    Colour codes change the colour of the text when displayed
    in a terminal, but are unnecessary when parsing the text
    in a script (like various functions in this module).
    '''
    result = ''
    in_colour_block = False
    for c in s:
        if c == '\x1b':
            in_colour_block = True
        elif in_colour_block and c == 'm':
            in_colour_block = False
        elif not in_colour_block:
            result += c
    return result


def tokenize(s, delim=' '):
    '''
    Given a string: remove colour codes, strip leading/trailing whitespace,
    split on the given delimiter and filter out empty elements (i.e. ignore when
    the delimeter appears multiple times in a row).
    '''
    return list(
        filter(lambda t: len(t) > 0,
               remove_colour_codes(s).strip().split(delim)))


def parse_timestamp(tokens):
    '''
    Parse the timestamp from the given tokens. This function assumes the tokens
    come from a line in our node logs and therefore that the first three
    elements form the date/time string.
    '''
    return datetime.datetime.strptime(' '.join([YEAR] + tokens[:3]), DATE_FORMAT_STR)


def parse_log_file_stateless(filename, determine_type, transform):
    '''
    Consume the given log file, on each line run the `determine_type` function.
    If the result is not none, then tokenize the line, extract the timestamp and
    run the `transform` function on the type+tokens to extract any additional
    information. Return the list of extracted times, types and data.
    
    Each line is considered individually, no state is kept between lines.
    '''
    result = []
    with open(filename) as f:
        for line in f.readlines():
            typ = determine_type(line)
            if typ is not None:
                tokens = tokenize(line)
                timestamp = parse_timestamp(tokens).timestamp()
                data = transform(typ, tokens)
                result.append({
                    'timestamp': timestamp,
                    'type': typ,
                    'data': data
                })
    return result

def parse_log_file_stateful(filename, determine_type, transform, update):
    '''
    Similar to `parse_log_file_stateless`, except an additional function is used
    to update a state machine persisted between lines. The `update` function
    takes the type, transform output and previous state. It returns the new
    state and whether it should be added to the output list or not.
    '''
    result = []
    state = None
    with open(filename) as f:
        for line in f.readlines():
            typ = determine_type(line)
            if typ is not None:
                tokens = tokenize(line)
                data = transform(typ, tokens)
                (state, add_to_result) = update(typ, data, state)
                if add_to_result:
                    result.append(state)
    return result
