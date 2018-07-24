import re
import logging


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
