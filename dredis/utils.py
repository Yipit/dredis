def to_float(s):
    # Redis uses `strtod` which converts empty string to 0
    if s == '':
        return 0
    else:
        return float(s)
