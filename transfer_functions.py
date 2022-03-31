def secure_dict_fetch(input_dict, key):
    """Turns empty strings and integers (0 and "0") into None. Returns all other values unchanged."""
    try:
        return input_dict[key]
    except KeyError:
        return None


def secure_dict_fetch_double(input_dict, key, key_2):
    """Turns empty strings and integers (0 and "0") into None. Returns all other values unchanged."""
    try:
        return input_dict[key][key_2]
    except KeyError:
        return None
