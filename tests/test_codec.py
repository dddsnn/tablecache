import sys

import pytest

import tablecache as tc


@pytest.mark.parametrize('s', ['', 'foo', 'äöüß'])
def test_str(s):
    encoded = tc.encode_str(s)
    assert isinstance(encoded, bytes)
    decoded = tc.decode_str(encoded)
    assert decoded == s


@pytest.mark.parametrize('i', [0, 1, -1, sys.maxsize + 1])
def test_int_as_str(i):
    encoded = tc.encode_int_as_str(i)
    assert isinstance(encoded, bytes)
    decoded = tc.decode_int_as_str(encoded)
    assert decoded == i
