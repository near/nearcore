import binascii

alphabet = b'123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'


def _b58encode_int(i):
    string = b''
    while i:
        i, idx = divmod(i, 58)
        string = alphabet[idx:idx + 1] + string
    return string


def b58encode(v):
    """
    :param v: bytes object or integer list representation of bytes
    :return: base58 encoded bytes
    """
    p, acc = 1, 0
    for c in reversed(list(bytearray(v))):
        acc += p * c
        p = p << 8

    result = _b58encode_int(acc)

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
