"""Account name generator

Provides tools to generate account names that are distributed evenly across the shards.

For account naming rules and conventions see https://nomicon.io/DataStructures/Account
"""

import os
import sys
import random
import re
import unittest


def char_range(lower, upper, upper_inclusive=True):
    l = ord(lower)
    u = ord(upper) + 1 if upper_inclusive else ord(upper)
    return [chr(i) for i in range(l, u)]


def alpha_num_str(length):
    return ''.join(
        random.choices(char_range('0', '9') + char_range('a', 'z'), k=length))


def random_char_below(upper, upper_inclusive):
    if upper >= 'a':
        chars = char_range('0', '9') + char_range('a', upper, upper_inclusive)
    elif upper == '_':
        chars = char_range('0', '9')
        if upper_inclusive:
            chars.append('_')
    elif upper >= '1':
        chars = char_range('0', upper, upper_inclusive)
    elif upper == '0':
        # here just return a - if upper_inclusive is False, since that's called
        # only when we want to finish a prefix of the upper string. Otherwise, don't bother
        # with returning a - to avoid handling account ID validity in that case
        if upper_inclusive:
            chars = ['0']
        else:
            chars = ['-']
    else:
        assert upper == '-' or upper == '.'
        return upper
    return random.choice(chars)


def random_char_above(lower):
    if lower >= 'a':
        chars = char_range(lower, 'z')
    elif lower == '_':
        chars = ['_'] + char_range('a', 'z')
    elif lower >= '0':
        chars = char_range(lower, '9') + char_range('a', 'z')
    elif lower == '.':
        chars = ['.'] + char_range('0', '9') + char_range('a', 'z')
    else:
        assert lower == '-'
        chars = ['-'] + char_range(lower, '9') + char_range('a', 'z')
    return random.choice(chars)


def random_valid_char(lower, upper, upper_inclusive=True):
    if lower is None and upper is None:
        chars = char_range('0', '9') + char_range('a', 'z')
        return random.choice(chars)
    if lower is None:
        return random_char_below(upper, upper_inclusive)
    if upper is None:
        return random_char_above(lower)

    assert (upper_inclusive and lower <= upper) or (not upper_inclusive and
                                                    lower < upper)

    if lower >= 'a':
        assert lower <= 'z'
        chars = char_range(lower, upper, upper_inclusive)
    elif lower == '_':
        chars = ['_']
        if upper != '_':
            chars += char_range('a', upper, upper_inclusive)
    elif lower >= '0':
        if upper <= '9':
            chars = char_range(lower, upper, upper_inclusive)
        elif upper == '_':
            chars = char_range(lower, '9')
            if upper_inclusive:
                chars += ['_']
        else:
            chars = char_range(lower, '9') + char_range('a', upper,
                                                        upper_inclusive)
    elif lower == '.':
        if upper == '.':
            # upper_inclusive must be false here because of the assert above
            chars = ['.']
        elif upper <= '9':
            assert upper >= '0'
            chars = ['.'] + char_range('0', upper, upper_inclusive)
        elif upper == '_':
            chars = ['.'] + char_range('0', '9')
            if upper_inclusive:
                chars += '_'
        else:
            assert upper >= 'a' and upper <= 'z'
            chars = ['.'] + char_range('0', '9') + char_range(
                'a', upper, upper_inclusive)
    else:
        assert lower == '-'
        if upper == '-' or upper == '.':
            chars = ['-']
            # if upper == '.', we don't include the '.' in the list of chars even if upper_inclusive
            # is True, because we want to make it easy for ourselves in the case we're generating a prefix
            # between b-0 and b.0, where if we choose a '.' in position 2, we're out of luck because the next
            # char is a 0
        elif upper <= '9':
            assert upper >= '0'
            chars = ['-'] + char_range('0', upper, upper_inclusive)
        elif upper == '_':
            chars = ['-'] + char_range('0', '9')
            if upper_inclusive:
                chars += '_'
        else:
            assert upper >= 'a' and upper <= 'z'
            chars = ['-'] + char_range('0', '9') + char_range(
                'a', upper, upper_inclusive)
    return random.choice(chars)


def char_at(s, i):
    if s is None or i >= len(s):
        return None
    return s[i]


# when picking a random char between two chars, we'll consider it as counting toward the number
# of free chars we wanted to generate if the range is large. We could try and be clever and add fractional
# "freeness" but it's not a big deal, and the initial choice of free_chars=6 is arbitrary anyway
def char_range_is_large(l, u):
    return (l is None or l < '4') and (u is None or u > 't')


# if we so far generated a prefix exactly equal to the upper boundary string up to its second to last char,
# handle it specially here, because we have to make sure we generate a prefix strictly lower than it
def finish_upper(lower, upper, prefix, free_chars, free_length):
    if len(upper) > 1:
        # e.g. lower = "aurora" and upper = "aurora-0". In that case "aurora" is the only valid account ID in the shard
        assert upper[-2] != '-' or upper[
            -1] != '0', f'Cannot build account ID prefix less than {upper}'
    else:
        assert upper[
            0] > '0', f'Cannot build account ID prefix less than {upper}'

    l = char_at(lower, len(upper) - 1)
    c = random_valid_char(l, upper[-1], upper_inclusive=False)
    prefix += c
    if char_range_is_large(l, upper[-1]):
        free_chars += 1
    if l is not None and c != l:
        # we've generated a prefix strictly greater than lower, so no need to consider it anymore
        lower = None
    extra_chars_needed = c in ['.', '-', '_'] or lower is not None
    if lower is not None:
        for i in range(len(upper), len(lower)):
            c = random_char_above(lower[i])
            prefix += c
            if char_range_is_large(lower[i], None):
                free_chars += 1
            if c != lower[i]:
                extra_chars_needed = False
                break
    if extra_chars_needed:
        extra_len = max(1, free_length - free_chars)
    else:
        extra_len = max(0, free_length - free_chars)
    if extra_len > 0:
        prefix += alpha_num_str(extra_len)
    return prefix


# generate a string that's a valid account ID between lower and upper
# free_length refers to the number of characters in the result that are free
# to be chosen from a large range. For example, if lower='aaa', upper='aaa0',
# then we want a string of length 4 + free_length, because the first 4 characters
# are constricted
# TODO: This could hopefully be made simpler by successively appending either an
# alphanumeric character or one of ['-', '.', '_'] followed by an alphanumeric character,
# choosing one of the ones that keeps us between the bounds each time.
# See https://github.com/near/nearcore/pull/9194#pullrequestreview-1488492798
def random_prefix_between(lower, upper, free_length=6):
    assert lower is None or upper is None or lower < upper, (lower, upper)

    # 1 shard case
    if lower is None and upper is None:
        return alpha_num_str(free_length)

    prefix = ''
    free_chars = 0
    max_len = 0
    if upper is not None:
        max_len = len(upper)
    if lower is not None:
        max_len = max(max_len, len(lower))

    for i in range(max_len):
        l = char_at(lower, i)
        u = char_at(upper, i)

        if l is not None and l == u:
            prefix += l
            continue

        if l is None and u is None:
            # we get here when lower is shorter than upper, and we have generated
            # a string equal to lower. Just add anything at the end
            extra_len = max(1, free_length - free_chars)
            prefix += alpha_num_str(extra_len)
            return prefix

        if upper is not None and i == len(upper) - 1:
            return finish_upper(lower, upper, prefix, free_chars, free_length)

        c = random_valid_char(l, u)
        prefix += c
        if char_range_is_large(l, u):
            free_chars += 1
        if c != u and c != l:
            if free_chars < free_length:
                prefix += alpha_num_str(free_length - free_chars)
            return prefix

        if c != u:
            upper = None
        if c != l:
            lower = None

        if free_chars >= free_length:
            # here only one of lower/upper is not None, meaning we have a prefix of it so far
            # if it's a prefix of upper, it's strictly smaller, since we call finish_upper() before hitting
            # the end of upper in this loop.
            # If it's a prefix of lower, we add chars here to make it larger. Just add z's
            # to be lazy and make the code simpler, since we already got the number of free chars we wanted
            if lower is not None:
                prefix += 'z'
                for j in range(i + 1, len(lower)):
                    if lower[j] != 'z':
                        break
                    prefix += 'z'
            return prefix

    assert lower is not None and upper is None
    # here we happened to generate a prefix equal to lower, but still strictly less than upper
    # no matter what we do, so just add anything to the end.
    extra_len = max(1, free_length - free_chars)
    prefix += alpha_num_str(extra_len)
    return prefix


def random_account_between(base_name, suffix, lower, upper, free_length=6):
    prefix = random_prefix_between(lower, upper, free_length)
    return f'{prefix}{suffix}.{base_name}'


# Given a shard layout, generates accounts distributed evenly across the shards
class AccountGenerator:

    def __init__(self, shard_layout):
        assert len(shard_layout) == 1
        assert 'V0' in shard_layout or 'V1' in shard_layout

        self.shard_map = {}

        # If the shard layout is V0, we can just skip this, and random_account_id()
        # will see an empty self.shard_map and generate a random prefix, which should
        # distribute the accounts evenly across shards in that case
        shard_layout_v1 = shard_layout.get('V1')
        if shard_layout_v1 is not None:
            # taken from a comment in core/account-id/src/lib.rs
            account_regex = re.compile(
                r'^(([a-z\d]+[-_])*[a-z\d]+\.)*([a-z\d]+[-_])*[a-z\d]+$')
            # doesn't actually matter that much to get the shard IDs right, since we're just
            # picking one at random, and not actually doing anything with the shard ID itself, but
            # add the right offset to the shard IDs below just for cleanliness, and in case we
            # want to print out shard IDs or something
            shard_offset = len(shard_layout_v1['fixed_shards'])
            accounts = shard_layout_v1['boundary_accounts']
            if len(accounts) == 0:
                self.shard_map[shard_offset] = (None, None)
                return

            if accounts[0] != '00':
                self.shard_map[shard_offset] = (None, accounts[0])

            for i, account_id in enumerate(accounts):
                # should of course be true, but assert it since we let the user pass a shard layout file, so
                # verify it at least a little bit, along with the check below that the list is increasing
                assert account_regex.fullmatch(account_id) is not None

                if i + 1 < len(accounts):
                    next_account = accounts[i + 1]
                    assert account_id < next_account

                    # like "aurora" and "aurora-0", in this case we can't generate any accounts there
                    if next_account != account_id + '-0':
                        self.shard_map[shard_offset + i + 1] = (account_id,
                                                                next_account)
                else:
                    if account_id != 'z' * 64:
                        self.shard_map[shard_offset + i + 1] = (account_id,
                                                                None)

            # This should be true no matter what boundary accounts we have, but just sanity check it
            assert len(self.shard_map) > 0

    # generate a valid subaccount ID of `base_name`` between lower and upper, with the first part of
    # the account ID ending with `suffix`
    # TODO: check the resulting length somewhere. Right now it's not checked and could be too large
    # if `base_name` is large
    def random_account_id(self, base_name, suffix):
        if len(self.shard_map) == 0:
            return random_account_between(base_name, suffix, None, None)
        else:
            shard_id, (lower,
                       upper) = random.choice(list(self.shard_map.items()))
            return random_account_between(base_name, suffix, lower, upper)


class TestRandomAccount(unittest.TestCase):

    def test_random_account(self):
        account_regex = re.compile(
            r'^(([a-z\d]+[-_])*[a-z\d]+\.)*([a-z\d]+[-_])*[a-z\d]+$')
        test_cases = [
            (None, None),
            ('aa', None),
            (None, 'aa'),
            ('aa', 'bb'),
            ('56', 'bb'),
            ('a-1', 'a-1-1'),
            ('a-0', 'a-01'),
            ('a-0', 'a-00'),
            ('b-b', 'bb'),
            ('aa', 'aa000'),
            ('b-0', 'b.0'),
        ]
        for (lower, upper) in test_cases:
            # sanity check the test case itself
            if lower is not None:
                assert account_regex.fullmatch(lower) is not None
            if upper is not None:
                assert account_regex.fullmatch(upper) is not None

            for _ in range(10):
                account_id = random_account_between('foo.near', '_ft', lower,
                                                    upper)
                assert account_regex.fullmatch(account_id) is not None, (
                    account_id, lower, upper)
                if lower is not None:
                    assert account_id >= lower, (account_id, lower, upper)
                if upper is not None:
                    assert account_id < upper, (account_id, lower, upper)
