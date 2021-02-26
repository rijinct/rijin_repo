#  """
#  @author: pranchak
#  """
import re


class SubpartitionRetriever:
    def __init__(self):
        self._target_table_name = ""

    @property
    def target_table_name(self):
        return self._target_table_name

    def get_subpartition(self, query):
        return self._get_sub_partitions_list(
                query)

    @staticmethod
    def _replace_brackets(s):
        if "(" in s:
            s = s[s.find("(") + 1:s.find(")")]
        return s

    @staticmethod
    def _reformat(partition):
        return re.sub("'", "", partition).strip()

    def _get_sub_partitions_list(self, parsed_query):
        target_table_name = ""
        sub_partitions_list = []
        for token in parsed_query.tokens:
            if token.value.startswith(
                    'partition(') or token.value.startswith(
                    '(dt='):
                partitions = SubpartitionRetriever._replace_brackets(token.value).split(",")
                for partition in partitions:
                    partition = SubpartitionRetriever._reformat(partition)
                    sub_partitions_list.append(partition)
            elif re.fullmatch("TABLE", token.value, re.IGNORECASE):
                self._target_table_name = parsed_query.token_next(
                        parsed_query.token_index(
                                token) + 1)[1].value
        return [SubpartitionRetriever._get_partition(partition_value) for
            partition_value in
            sub_partitions_list]

    @staticmethod
    def _get_partition(partition):
        if '=' in partition:
            partition = '{}={}'.format(partition.split('=')[0].lower(),
                                       partition.split('=')[1])
        else:
            partition = partition.lower()
        return partition
