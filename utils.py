import re


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
    if match:
        return len(match.group(0))
    return 0
