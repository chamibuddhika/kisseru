import re
import logging

################### String Utilities ######################


def get_file_name(infile):
    if infile != None:
        tokens = infile.split('/')
        return tokens[-1]
    return None


def get_path_to_file(infile):
    if infile != None:
        tokens = infile.split('/')
        return ''.join(tokens[:len(tokens) - 1])
    return None


def get_file_name_without_extention(infile):
    if infile != None:
        tokens = infile.split('.')
        return ''.join(tokens[:len(tokens) - 1])
    return None


def get_file_extention(infile):
    if infile != None and type(infile) == str:
        tokens = infile.split('.')
        if len(tokens) > 1:
            return tokens[-1]
    return None


def gen_spaces(n_spaces):
    spaces = ''
    return ''.join(map(lambda _: ' ', range(n_spaces)))


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


def remove_suffix(text, suffix):
    if not text.endswith(suffix):
        return text
    return text[:len(text) - len(suffix)]


def get_indentation(line):
    whitespace_regex = re.compile('\s+')
    match = whitespace_regex.search(line)
    indent = 0
    if match:
        for c in match.group(0):
            if c == '\t':
                indent += 4
            else:
                indent += 1
        return indent
    return 0


################### Data Type Utilities ######################


def gen_tuple(ls):
    if ls == None or type(ls) != list:
        return ()

    if not len(ls):
        return ()
    elif len(ls) > 10:
        return None

    length = len(ls)
    switcher = {
        1: (ls[0]),
        2: (ls[0], ls[1]),
        3: (ls[0], ls[1], ls[2]),
        4: (ls[0], ls[1], ls[2], ls[3]),
        5: (ls[0], ls[1], ls[2], ls[3], ls[4]),
        6: (ls[0], ls[1], ls[2], ls[3], ls[4], ls[5]),
        7: (ls[0], ls[1], ls[2], ls[3], ls[4], ls[5], ls[6]),
        8: (ls[0], ls[1], ls[2], ls[3], ls[4], ls[5], ls[6], ls[7]),
        9: (ls[0], ls[1], ls[2], ls[3], ls[4], ls[5], ls[6], ls[7], ls[8]),
        10: (ls[0], ls[1], ls[2], ls[3], ls[4], ls[5], ls[6], ls[7], ls[8],
             ls[9])
    }
    return switcher.get(length, None)


################### Logging Utilities ######################


def _set_log(log, fmt):
    propagate = False
    log.propagate = propagate
    logger_handler = logging.StreamHandler()
    log.addHandler(logger_handler)
    logger_handler.setFormatter(logging.Formatter(fmt))
    return (logger_handler, propagate)


def _reset_log(log, handler, propagate):
    log.removeHandler(handler)
    log.propagate = propagate


def logf_critical(log, fmt, msg):
    h, p = _set_log(log, fmt)
    log.critical(msg)
    _reset_log(log, h, p)


def logf_error(log, fmt, msg):
    h, p = _set_log(log, fmt)
    log.error(msg)
    _reset_log(log, h, p)


def logf_warning(log, fmt, msg):
    h, p = _set_log(log, fmt)
    log.warning(msg)
    _reset_log(log, h, p)


def logf_info(log, fmt, msg):
    h, p = _set_log(log, fmt)
    log.info(msg)
    _reset_log(log, h, p)


def logf_debug(log, fmt, msg):
    h, p = _set_log(log, fmt)
    log.debug(msg)
    _reset_log(log, h, p)


def logp_critical(log, msg):
    logf_critical(log, '%(message)s', msg)


def logp_error(log, msg):
    logf_error(log, '%(message)s', msg)


def logp_warning(log, msg):
    logf_warning(log, '%(message)s', msg)


def logp_info(log, msg):
    logf_info(log, '%(message)s', msg)


def logp_debug(log, msg):
    logf_debug(log, '%(message)s', msg)
