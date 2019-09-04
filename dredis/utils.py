import logging
import struct
import sys


def to_float(s):
    # Redis uses `strtod` which converts empty string to 0
    if s == '':
        return 0
    else:
        return float(s)


class FloatCodec(object):
    """
    FloatCode encodes `float` objects to bytes and preserve their numerical order.
    For example, the following list is encoded to bytes while keeping and preserves its numerical order:
    >>> floats = [1.5, -2.5, 3.2, 2.0]
    >>> sorted(floats, key=FloatCodec().encode)
    [-2.5, 1.5, 2.0, 3.2]

    References:
    * https://en.wikipedia.org/wiki/Floating-point_arithmetic#IEEE_754_design_rationale
    * https://stackoverflow.com/a/43305015/565999
    * https://stackoverflow.com/a/12933766/565999
    * https://ananthakumaran.in/2018/08/17/order-preserving-serialization.html#float
    * https://github.com/apple/foundationdb/blob/b92e6b09ad67fa382e17500536fd13b10bb23ede/design/tuple.md#ieee-binary-floating-point
    """

    STRUCT = struct.Struct('>d')

    def encode(self, score):
        score_bytes = bytearray(self.STRUCT.pack(score))
        # if a negative number, flip all bits
        if score_bytes[0] & 0x80 != 0x00:
            score_bytes = self._flip_bits(score_bytes)
        else:
            # flip the sign if it's a positive number
            score_bytes[0] ^= 0x80  # flip the sign
        return bytes(score_bytes)

    def decode(self, bytestring):
        score_bytes = bytearray(bytestring)
        # if a negative number, flip all bits
        if score_bytes[0] & 0x80 != 0x80:
            score_bytes = self._flip_bits(score_bytes)
        else:
            # flip the sign if it's a positive number
            score_bytes[0] ^= 0x80  # flip the sign
        return self.STRUCT.unpack(score_bytes)[0]

    def _flip_bits(self, bytestring):
        for i in xrange(len(bytestring)):
            bytestring[i] ^= 0xff
        return bytestring


FLOAT_CODEC = FloatCodec()


def setup_logging(level):
    logger = logging.getLogger('dredis')
    logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
