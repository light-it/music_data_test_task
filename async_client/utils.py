
def httpize(d):
    if d is None:
        return None
    converted = {}
    for k, v in d.items():
        if isinstance(v, bool):
            v = "1" if v else "0"
        if not isinstance(v, str):
            v = str(v)
        converted[k] = v
    return converted

