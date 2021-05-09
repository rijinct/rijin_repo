class StringUtils:

    LINE_SEPARATOR = '\n'

    WORD_SEPARATOR = ' '

    @staticmethod
    def get_first_line(
            string, separator=LINE_SEPARATOR):
        return string.split(separator)[0]

    @staticmethod
    def get_last_line(
            string, separator=LINE_SEPARATOR):
        return string.split(separator)[-1]

    @staticmethod
    def get_word(
            string, separator=WORD_SEPARATOR, index=0):
        return string.split(separator)[index]

    @staticmethod
    def get_last_word(
            string, separator=WORD_SEPARATOR):
        return string.split(separator)[-1]
