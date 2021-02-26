#  """
#  @author: pranchak
#  """

import re

import sqlparse
from sqlparse.sql import Comment, Identifier, Parenthesis, \
    IdentifierList, \
    Where


class QueryParser:
    def __init__(self, sql_query):
        self.parsed_query = QueryParser._remove_distribute_by(sql_query)
        query = QueryParser._remove_insert_stmt(sql_query)
        query = self._remove_where_clause(query)
        self._query = self._replace_table_name_with_view_name(query)

    @property
    def query(self):
        return self._query

    @staticmethod
    def _remove_distribute_by(parsed_query):
        for token in parsed_query.tokens:
            if re.search("distribute", token.value, re.IGNORECASE):
                parsed_query.tokens.remove(
                    parsed_query.token_next(parsed_query.token_index(
                        token) + 2)[1])
                parsed_query.tokens.remove(
                    parsed_query.token_next(parsed_query.token_index(
                        token) + 1)[1])
                parsed_query.tokens.remove(token)
        return parsed_query

    @staticmethod
    def _remove_insert_stmt(parsed_query):
        tokens = []
        flag = False
        for token in parsed_query:
            if token.value.upper() == "SELECT":
                flag = True
                tokens.append(token.value)
            elif flag and not isinstance(token, Comment):
                tokens.append(token.value)
        return "".join(tokens)

    def _remove_where_clause(self, sql_query):
        parsed_query = self.parsed_query
        where_cond = self._get_where_clause(parsed_query)
        for cond in where_cond:
            updated_cond = QueryParser._get_updated_boundary_condition(cond)
            sql_query = sql_query.replace(cond, updated_cond)
        return sql_query

    def _get_where_clause(self, parsed_query):
        where_cond = []
        for token in parsed_query:
            if isinstance(token, Identifier) \
                    or isinstance(token, Parenthesis) \
                    or isinstance(token, IdentifierList) \
                    or isinstance(token, sqlparse.sql.Function):
                where_cond.extend(self._get_where_clause(token))
            elif isinstance(token, Where):
                where_cond.append(token.value)
        return where_cond

    @staticmethod
    def _get_updated_boundary_condition(condition):
        updated_where_clause = []
        updated_condition = ""
        cond_split = re.split(r"\band\b", condition, flags=re.IGNORECASE)
        for cond in cond_split:
            if not (re.match(r".*?dt\s*?>=\s*?'\d+'", cond,
                             flags=re.IGNORECASE)
                    or re.match(r".*?dt\s*?<\s*?'\d+'", cond,
                                flags=re.IGNORECASE)
                    or re.match(r".*?tz\s*?=\s*?'.+?'", cond,
                                flags=re.IGNORECASE)):
                updated_where_clause.append(cond)
        if updated_where_clause:
            updated_condition += " AND ".join(updated_where_clause)
            if not re.match(r"^(where)", updated_condition,
                            flags=re.IGNORECASE):
                updated_condition = "WHERE" + updated_condition
        return updated_condition

    def _replace_table_name_with_view_name(self, sql_query):
        WORDS = {"FROM", "LEFT OUTER JOIN", "JOIN", "RIGHT OUTER JOIN"}
        parsed_query = self.parsed_query
        token_value_list = parsed_query.flatten()
        is_table_name_replacement_needed = False
        for i in token_value_list:
            if i.value.upper() in WORDS:
                is_table_name_replacement_needed = True
            elif QueryParser._is_blank(i.value):
                pass
            elif i.value == '(':
                is_table_name_replacement_needed = False
            elif is_table_name_replacement_needed:
                table_name = i.value.strip()
                view_name = QueryParser._get_view_name(table_name)
                sql_query = \
                    QueryParser._case_insensitive_replace_table_name(
                        table_name, view_name, sql_query)
                is_table_name_replacement_needed = False

        return sql_query.strip()

    @staticmethod
    def _is_blank(string):
        return re.match("\s", string)

    @staticmethod
    def _get_view_name(name):
        name = name.split("_")
        return "{}1_{}".format(name[0], "_".join(name[1:]))

    def _case_insensitive_replace_table_name(old_name, new_name, string):
        expr = re.compile(r'from\s{}'.format(old_name), re.IGNORECASE)
        view_name_with_alias = "from {new_name} {old_name}".format(
            new_name=new_name, old_name=old_name)
        return expr.sub(view_name_with_alias, string)
