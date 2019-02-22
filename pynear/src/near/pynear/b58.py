import binascii

alphabet = b'123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'


def b58encode_int(i, default_one=True):
    if not i and default_one:
        return alphabet[0:1]

    string = b''
    while i:
        i, idx = divmod(i, 58)
        string = alphabet[idx:idx + 1] + string
    return string


def b58encode(v):
    p, acc = 1, 0
    for c in reversed(list(bytearray(v))):
        acc += p * c
        p = p << 8

    result = b58encode_int(acc, default_one=False)

    return result


def b58decode(s):
    if not s:
        return b''

    # Convert the string to an integer
    n = 0
    for char in s.encode('utf-8'):
        n *= 58
        if char not in alphabet:
            msg = "Character {} is not a valid base58 character".format(char)
            raise Exception(msg)

        digit = alphabet.index(char)
        n += digit

    # Convert the integer to bytes
    h = '%x' % n
    if len(h) % 2:
        h = '0' + h
    res = binascii.unhexlify(h.encode('utf8'))

    # Add padding back.
    pad = 0
    for c in s[:-1]:
        if c == alphabet[0]:
            pad += 1
        else:
            break

    return b'\x00' * pad + res
