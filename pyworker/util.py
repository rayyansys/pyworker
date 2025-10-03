import time
import datetime
import dateutil.relativedelta

def get_current_time():
    # TODO return timezone or utc? get config from user?
    return datetime.datetime.utcnow()

def get_time_delta(**kwargs):
    return dateutil.relativedelta.relativedelta(**kwargs)

def squash_multiline_yaml(lines):
    """
    Given a list of YAML lines, squash lines with unclosed quotes (single or double)
    into a single line until the quote closes.
    Handles values that contain ':' safely.
    """
    squashed = []
    buffer = None
    quote_char = None

    for line in lines:
        stripped = line.strip()

        if buffer is None:
            if ":" in line:
                # split only once (key : value)
                key, val = line.split(":", 1)
                val = val.lstrip()

                # quoted start but not closed
                if val.startswith("'") and not (len(val) > 1 and val.endswith("'")):
                    buffer = line.rstrip("\n")
                    quote_char = "'"
                    continue
                elif val.startswith('"') and not (len(val) > 1 and val.endswith('"')):
                    buffer = line.rstrip("\n")
                    quote_char = '"'
                    continue

            squashed.append(line)

        else:
            # still accumulating
            buffer += "\\n" + line.strip("\n")

            # closing quote?
            if line.strip().endswith(quote_char):
                squashed.append(buffer)
                buffer = None
                quote_char = None

    # in case YAML was malformed and never closed
    if buffer is not None:
        squashed.append(buffer)

    return squashed
