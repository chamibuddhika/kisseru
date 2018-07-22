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
    indent = 0
    if match:
        for c in match.group(0):
            if c == '\t':
                indent += 4
            else:
                indent += 1
        return indent
    return 0
