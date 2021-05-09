'''
Created on 09-Apr-2020

@author: nageb
'''

QUOTES = ('\'', '"')


def convert_str_to_data_structure(input_string, delimiter=None):
    column_names, rows, delimiter = \
        _fetch_input_details(input_string, delimiter)
    return _obtain_output(column_names, rows, delimiter)


def substring_after(substring, string, occurrences=1):
    if substring in string:
        return substring.join(
            string.split(substring, occurrences)[occurrences:])
    else:
        return string


def substring_before(substring, string, occurrences=1):
    if substring in string:
        return substring.join(
            string.split(substring, occurrences)[:occurrences])
    else:
        return string


def get_index(substring, string, occurrences=1):
    index = -1
    for occurence in range(occurrences):
        index = string.index(substring, index + 1)
    return index


def is_quoted(string):
    return string[0] in QUOTES and \
            string[-1] in QUOTES


def remove_quotes(string):
    return string[:-1][1:]


def _fetch_input_details(input_string, delimiter):
    lines = input_string.strip().split("\n")
    header = lines[0]
    if delimiter is None:
        delimiter = _determine_delimiter(header, "|,")
    column_names = tuple(map(lambda x: x.strip(), header.split(delimiter)))
    rows = lines[1:]
    return column_names, rows, delimiter


def _determine_delimiter(line, delimiters, default=","):
    for delimiter in delimiters:
        if delimiter in line:
            return delimiter
    return default


def _obtain_output(column_names, rows, delimiter):
    output = []
    for row in rows:
        output_row = _process_line(column_names, row, delimiter)
        output.append(output_row)

    return output


def _process_line(column_names, row, delimiter):
    output_row = {}
    values = tuple(map(lambda x: x.strip(), row.split(delimiter)))
    for col_num, col_name in enumerate(column_names):
        output_row[col_name] = values[col_num]

    return output_row
