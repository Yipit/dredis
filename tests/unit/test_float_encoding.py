from dredis.utils import FLOAT_CODEC


def test_encoding_keeps_natural_order():
    floats = [1.5, -2.5, 0, 3.2, -2.0]
    assert sorted(floats, key=FLOAT_CODEC.encode) == sorted(floats)


def test_decoding_should_be_the_opposite_of_encoding():
    floats = [1.5, -2.5, 0, 3.2, -2.0]
    encoded_floats = map(FLOAT_CODEC.encode, floats)
    assert map(FLOAT_CODEC.decode, encoded_floats) == floats
