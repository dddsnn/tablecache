def _encode_stringified(value):
    return str(value).encode()


def encode_str(s):
    return s.encode()


def decode_str(bs):
    return bs.decode()


encode_int_as_str = _encode_stringified


def decode_int_as_str(bs):
    return int(bs.decode())
