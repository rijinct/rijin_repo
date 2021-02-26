import re

from connector import SparkConnector
from reader import ReaderAndViewCreator
from specification import TargetSpecification


class QueryGenerator:
    commn_dict = {
        'string': "'Unknown'",
        'int': -10,
        'bigint': -10,
        'double': -10.0,
        'decimal': -10.0,
        'array': "array(cast(-10 as bigint))"
    }

    NULL_HANDLING_CASE_COND = '''nvl(case when {0}='null' then NULL else {0} end,{1}) as {2}'''  # noqa: E501
    NULL_HANDLING_NVL = '''nvl({0},{1}) as {2}'''
    LEFT_OUTER_JOIN_CONDITION = 'left outer join (select * from {0} ) {1}_{' \
                                '2}  on {3}'
    LEFT_OUTER_JOIN_WITH_FILTER = 'left outer join (select * from {0} where ' \
                                  '{3}) {1}_{' \
                                  '2} on {4}'
    JOIN_QUERY = 'select {} from {} {} {}'
    JOIN_QUERY_WITH_BROADCAST = 'select /*+ BROADCASTJOIN( {0} )*/ {1} from ' \
                                '{2} ' \
                                '{3} {4}'
    LEFT_OUTER_JOIN_WITH_DISTINCT = 'left outer join (select {0} from ' \
                                    '{1} group by {0}) ' \
                                    '{2}_{3}  on {4}'
    LEFT_OUTER_JOIN_WITH_FILTER_DISTINCT = 'left outer join (select {0} ' \
                                           'from {1} where {4} ' \
                                           'group by {0}) {2}_{3} on {5}'
    SEPARATOR = \
        '+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'

    def __init__(self, target_spec: TargetSpecification,
                 reader_objs: [ReaderAndViewCreator],
                 addnl_reader_objs: [ReaderAndViewCreator]):
        self._target_spec = target_spec
        self._reader_objs = reader_objs
        self._addnl_reader_objs = addnl_reader_objs
        self._logger = SparkConnector.get_logger(__name__)

    def generate_query(self, spec):
        self._spec = spec
        query = self._spec['sql']['query'].replace('  ', ' ')
        inner_queries, inner_alias = self._apply_join_with_source()
        for iqtbl, iq in inner_queries.items():
            query = self._replace_string(
                ' {} {} '.format(inner_alias[iqtbl], iqtbl.upper()),
                ' ({}) {} '.format(iq, iqtbl.upper()), query)
        outer_query = self._apply_join_with_target(spec)
        if outer_query:
            query = outer_query.format(query)
        self._logger.info('Final Query')
        self._logger.info(self.SEPARATOR)
        self._logger.info(query)
        return query

    def _apply_join_with_source(self):
        self._logger.info("{}\nSOURCE\n{}".format(self.SEPARATOR,
                                                  self.SEPARATOR))
        inner_queries = {}
        inner_alias = {}
        for reader_obj in self._reader_objs:
            ltbl, db_ltbl = self.__get_table_names(reader_obj.source_spec)
            ltbl_columns = self.__add_alias_to_col(ltbl, reader_obj.df.columns)
            query = self._iterate_addnl_reader_objs(
                ltbl, ltbl_columns, reader_obj.df.dtypes, 'source_table',
                reader_obj._get_view_name())
            if query:
                inner_queries[ltbl] = query
                inner_alias[ltbl] = reader_obj._get_view_name()
        return inner_queries, inner_alias

    def __get_table_names(self, spec):
        tbl = self._replace_string("{}.".format(spec.hive_schema), "",
                                   spec.get_table_name()).lower()
        db_tbl = spec.get_table_name()
        return tbl, db_tbl

    def __add_alias_to_col(self, table, columns):
        tbl_columns = []
        for col in columns:
            tbl_columns.append('{0}.{1} as {1}'.format(table, col))
        return tbl_columns

    def _iterate_addnl_reader_objs(self,
                                   ltbl,
                                   ltbl_columns,
                                   ltbl_dtypes,
                                   table_type,
                                   ltbl_view=''):
        extensions = self._reader_objs[0].source_spec.extensions
        join_conditions = []
        join_types = set()
        for addnl_reader_obj in self._addnl_reader_objs:
            rtbl, db_rtbl = self.__get_table_names(
                addnl_reader_obj.source_spec)
            rtbl_columns = addnl_reader_obj.df.columns
            if (rtbl in extensions and 'table_relationship' in self._spec
                    and ltbl in self._spec['table_relationship']
                    and rtbl in self._spec['table_relationship'][ltbl]):
                ltbl_columns, join_conditions, join_types = self._iterate_jointypes(  # noqa: E501
                    ltbl, ltbl_columns, rtbl, rtbl_columns, table_type,
                    ltbl_dtypes, join_conditions, join_types, extensions,
                    addnl_reader_obj._get_view_name())
        return self._get_join_query(ltbl, ltbl_columns, join_conditions,
                                    table_type, ltbl_view, join_types)

    def _iterate_jointypes(self, ltbl, ltbl_columns, rtbl, rtbl_columns,
                           table_type, ltbl_dtypes, join_conditions,
                           join_types, extensions, view_name):
        for jointype in extensions[rtbl]:
            if jointype.startswith('jointype_'):
                jc, cniq, onh, fc, dn = self._apply_join_by_type(
                    ltbl, rtbl, jointype, table_type, extensions)
                if jc:
                    ltbl_columns, jc = \
                        self._get_join_query_details(
                            ltbl, rtbl, ltbl_dtypes, jointype,
                            table_type,
                            ltbl_columns, rtbl_columns, jc,
                            cniq,
                            onh, fc,
                            view_name, dn)
                    join_conditions.append(jc)
                    if jc and rtbl not in self._get_heavy_es_tables():
                        join_types.add('{}_{}'.format(rtbl, jointype))
        return ltbl_columns, join_conditions, join_types

    def _apply_join_by_type(self, ltbl, rtbl, jointype, table_type,
                            extensions):
        self._logger.info(
            'Apply join by type {0} for {1} table {2} and {3}'.format(
                jointype, table_type, ltbl, rtbl))
        jc = ''
        cniq = {}
        onh = {}
        fc = ''
        dn = False
        if ('join_condition' in extensions[rtbl][jointype]
                and '{}.'.format(table_type)
                in extensions[rtbl][jointype]['join_condition']):
            jc = self._replace_string(
                '{}'.format(table_type), ltbl,
                extensions[rtbl][jointype]['join_condition'])
            jc = self._replace_string('{}.'.format(rtbl),
                                      '{}_{}.'.format(rtbl, jointype), jc)
            cniq, onh, fc, dn = self.__fill_join_tags(
                extensions[rtbl][jointype], ltbl, cniq, onh, fc, dn)
        if (table_type in extensions[rtbl][jointype]
                and ltbl in extensions[rtbl][jointype][table_type]):
            if ('join_condition'
                    in extensions[rtbl][jointype][table_type][ltbl]
                    and '{}.'.format(table_type) in extensions[rtbl][jointype]
                    [table_type][ltbl]['join_condition']):
                jc = self._replace_string(
                    '{}'.format(table_type), ltbl, extensions[rtbl][jointype]
                    [table_type][ltbl]['join_condition'])
                jc = self._replace_string('{}.'.format(rtbl),
                                          '{}_{}.'.format(rtbl, jointype), jc)
            cniq, onh, fc, dn = self.__fill_join_tags(
                extensions[rtbl][jointype][table_type][ltbl], ltbl, cniq, onh,
                fc, dn)
        return jc, cniq, onh, fc, dn

    def __fill_join_tags(self, join_dict, ltbl, cniq, onh, fc, dn):
        if 'columns_needed_in_query' in join_dict:
            cniq = join_dict['columns_needed_in_query']
        if 'override_null_handling' in join_dict:
            onh = join_dict['override_null_handling']
        if 'filter_condition' in join_dict:
            fc = self._process_filter_condition(join_dict['filter_condition'],
                                                ltbl)
        if 'distinct_needed' in join_dict:
            dn = join_dict['distinct_needed']
        return cniq, onh, fc, dn

    def _process_filter_condition(self, condition, table):
        condition = self._replace_string(r"{}.".format(table), "", condition)
        condition = self._replace_string("#LOWER_BOUND",
                                         str(self._target_spec.lower_bound),
                                         condition)
        return self._replace_string("#UPPER_BOUND",
                                    str(self._target_spec.upper_bound),
                                    condition)

    def _get_join_query_details(self, ltbl, rtbl, ltbl_dtypes, jointype,
                                table_type, ltbl_columns, rtbl_columns,
                                join_condition, columns_needed_in_query,
                                override_null_handling, filter_condition,
                                rtbl_view, distinct_needed):
        join_columns = ''
        is_any_one_column_present = False
        self._logger.info(
            'Over ride null handling {}'.format(override_null_handling))
        for litem, ritem in columns_needed_in_query.items():
            ltbl_col = '{0}.{1} as {1}'.format(ltbl, litem)
            if ltbl_col in ltbl_columns:
                is_any_one_column_present = True
                self._update_ltbl_col(jointype, litem, ltbl, ltbl_columns,
                                      ltbl_dtypes, override_null_handling,
                                      ritem, rtbl, rtbl_columns)

        if is_any_one_column_present:
            join_columns = self._get_join_columns(columns_needed_in_query,
                                                  distinct_needed,
                                                  filter_condition,
                                                  join_condition, jointype,
                                                  rtbl, rtbl_view)

            self._add_needed_logs(columns_needed_in_query, filter_condition,
                                  join_columns, join_condition, jointype, ltbl,
                                  ltbl_columns, rtbl, rtbl_columns, table_type)
        return ltbl_columns, join_columns

    def _update_ltbl_col(self, jointype, litem, ltbl, ltbl_columns,
                         ltbl_dtypes, override_null_handling, ritem, rtbl,
                         rtbl_columns):
        for rcol in rtbl_columns:
            regex = re.compile(r"\b{}\b".format(rcol), re.IGNORECASE)
            ritem = regex.sub(
                r"{}_{}.{}".format(rtbl, jointype, ritem), ritem)
        ind = ltbl_columns.index('{0}.{1} as {1}'.format(ltbl, litem))
        nh_cond = self.NULL_HANDLING_CASE_COND
        data_type = re.findall(r'\w+', ltbl_dtypes[ind][1])[0].lower()
        if data_type not in ['string']:
            nh_cond = self.NULL_HANDLING_NVL
        if litem in override_null_handling:
            ltbl_columns[ind] = nh_cond.format(
                ritem, override_null_handling[litem], litem)
        elif data_type in self.commn_dict.keys():
            ltbl_columns[ind] = nh_cond.format(
                ritem, self.commn_dict[data_type], litem)
        else:
            ltbl_columns[ind] = '{} as {}'.format(ritem, litem)

    def _get_join_columns(self, columns_needed_in_query, distinct_needed,
                          filter_condition, join_condition, jointype, rtbl,
                          rtbl_view):
        join_columns = self.LEFT_OUTER_JOIN_CONDITION.format(rtbl_view, rtbl,
                                                             jointype,
                                                             join_condition)  # noqa: E501
        if filter_condition:
            join_columns = self.LEFT_OUTER_JOIN_WITH_FILTER.format(
                rtbl_view, rtbl, jointype, filter_condition,
                join_condition)  # noqa: E501
        join_columns = \
            self._get_join_query_with_columns(
                columns_needed_in_query,
                rtbl_view, rtbl, jointype,
                join_condition,
                filter_condition) if distinct_needed else join_columns
        return join_columns

    def _add_needed_logs(self, columns_needed_in_query, filter_condition,
                         join_columns, join_condition, jointype, ltbl,
                         ltbl_columns, rtbl, rtbl_columns, table_type):
        self._logger.info('Join {0} details for {1} table {2} and {3}'.format(
            jointype, table_type, ltbl, rtbl))

        self._logger.info(self.SEPARATOR)
        self._logger.info('Right table columns {}'.format(rtbl_columns))
        self._logger.info('Join condition {}'.format(join_condition))
        self._logger.info(
            'Columns needed in query dict {}'.format(columns_needed_in_query))
        self._logger.info('Filter condition {}'.format(filter_condition))
        self._logger.info('Updated left table columns {}'.format(ltbl_columns))
        self._logger.info('Updated join condition {}'.format(join_columns))

    def _get_join_query(self,
                        ltbl,
                        select_columns,
                        join_conditions,
                        table_type,
                        ltbl_view='',
                        join_types=[]):
        query = ''
        if join_conditions:
            if table_type == 'source_table':
                join_conditions, query, select_columns = \
                    self._get_join_conditions_for_source(join_conditions,
                                                         join_types, ltbl,
                                                         ltbl_view, query,
                                                         select_columns)
            elif table_type == 'target_table':
                query = self._get_join_query_for_target_table(join_conditions,
                                                              ltbl, query,
                                                              select_columns)
            self._logger.info('Join for {0} table {1}'.format(
                table_type, ltbl))
            self._logger.info(self.SEPARATOR)
            self._logger.info('Select columns {}'.format(select_columns))
            self._logger.info('All Join conditions {}'.format(join_conditions))
            self._logger.info('Generated Query {}'.format(query))
        return query

    def _get_join_query_for_target_table(self, join_conditions, ltbl, query,
                                         select_columns):
        aselect_cols = []
        ajoin_conds = []
        for sc in select_columns:
            aselect_cols.append(
                self._replace_string('{}.'.format(ltbl),
                                     '{}_view.'.format(ltbl), sc))
        for jc in join_conditions:
            ajoin_conds.append(
                self._replace_string('{}.'.format(ltbl),
                                     '{}_view.'.format(ltbl), jc))
        sel_cols = ','.join(aselect_cols)
        join_conds = ' '.join(ajoin_conds)
        query = 'select {0} from ({1}) {2}_view {3}'.format(
            sel_cols, '{}', ltbl, join_conds)
        return query

    def _get_join_conditions_for_source(self, join_conditions, join_types,
                                        ltbl,
                                        ltbl_view, query, select_columns):
        spec = self._target_spec.yaml_spec[0]
        select_columns = self._fill_udf_in_select_columns(
            ltbl, spec, select_columns)
        sel_cols = ','.join(select_columns)
        join_conditions = self._fill_udf_in_join_columns(
            ltbl, spec, join_conditions)
        join_conds = ' '.join(join_conditions)
        query = self.JOIN_QUERY.format(
            sel_cols, ltbl_view, ltbl, join_conds)
        if join_types:
            query = self.JOIN_QUERY_WITH_BROADCAST.format(
                ','.join(join_types), sel_cols, ltbl_view, ltbl,
                join_conds)  # noqa: E501, E261
        return join_conditions, query, select_columns

    def _fill_udf_in_select_columns(self, ltbl, spec, select_columns):
        if 'table_udf_mapping' in spec and ltbl in spec['table_udf_mapping']:
            for udf_col, udf_def in spec['table_udf_mapping'][ltbl].items(
            ):  # noqa: E501
                col = '{0}.{1} as {1}'.format(ltbl, udf_col.lower())
                ind = QueryGenerator._get_udf_col_ind_select(
                    col, select_columns)
                if (ind > -1):
                    select_columns[ind] = '{0} as {1}'.format(
                        udf_def, udf_col.lower())
        return select_columns

    @staticmethod
    def _get_udf_col_ind_select(col, select_columns):
        res = -1
        if (col in select_columns):
            res = select_columns.index(col)
        else:
            col = ' '.join(col.split(' ')[-2:])
            for scol in select_columns:
                if re.search(' {}'.format(col), scol):
                    res = select_columns.index(scol)
                    break
        return res

    def _fill_udf_in_join_columns(self, ltbl, spec, join_conditions):
        if 'table_udf_mapping' in spec and ltbl in spec['table_udf_mapping']:
            for udf_col, udf_def in spec['table_udf_mapping'][ltbl].items():  # noqa: E501
                col = '{0}.{1}'.format(ltbl, udf_col.lower())
                join_conditions = [
                    self._replace_join_condition(col, udf_def, x, ltbl) for x
                    in
                    join_conditions]
        return join_conditions

    def _replace_join_condition(self, col, udf_def, x, ltbl):
        return self._replace_string(col, udf_def,
                                    x) if col in x and ltbl in x else x

    def _apply_join_with_target(self, spec):
        self._logger.info("{}\nTARGET\n{}".format(self.SEPARATOR,
                                                  self.SEPARATOR))
        ltbl, db_ltbl = self.__get_table_names(self._target_spec)
        schema = SparkConnector.spark_session.get_schema(db_ltbl)
        ltbl_df = SparkConnector.spark_session.createDataFrame([], schema)
        ltbl_columns = []
        partitions_list = self._get_partition_cols()
        for col in ltbl_df.df.columns:
            if col not in partitions_list:
                ltbl_columns.append('{0}.{1} as {1}'.format(ltbl, col))
        query = self._iterate_addnl_reader_objs(ltbl, ltbl_columns,
                                                ltbl_df.df.dtypes,
                                                'target_table')
        return query

    def _get_partition_cols(self):
        partitions_list = set()
        if 'partition' in self._spec:
            for sub_partition_col in self._spec['partition']['columns']:
                partitions_list.add(sub_partition_col)
        self._logger.info('partitions_list {}'.format(partitions_list))
        return partitions_list

    def _get_join_query_with_columns(self, columns_needed_in_query, rtbl_view,
                                     rtbl, jointype, join_condition,
                                     filter_condition):
        columns_in_jc = re.findall(
            r"{}_{}\.([a-zA-Z\d\-\_]+)".format(rtbl, jointype), join_condition)
        columns_needed_in_group_by = ",".join(
            set(columns_in_jc + list(columns_needed_in_query.values())))
        self._logger.info(
            'columns_needed_in_group_by {}'.format(
                columns_needed_in_group_by))

        join_columns = self.LEFT_OUTER_JOIN_WITH_DISTINCT.format(
            columns_needed_in_group_by, rtbl_view, rtbl, jointype,
            join_condition)  # noqa: E501
        if filter_condition:
            join_columns = self.LEFT_OUTER_JOIN_WITH_FILTER_DISTINCT.format(
                            columns_needed_in_group_by, rtbl_view, rtbl,
                            jointype, filter_condition, join_condition)
        return join_columns

    def _get_heavy_es_tables(self):
        h_es_tables = []
        if 'GlobalSettings' in self._spec and 'LARGE_ES_TABLES' in self._spec[
                'GlobalSettings']:  # noqa: E501
            h_es_tables = self._spec['GlobalSettings']['LARGE_ES_TABLES']
        return h_es_tables

    @staticmethod
    def _replace_string(string_present, string_to_be_replaced, input_string):
        replacement_string = re.compile(string_present, re.IGNORECASE)
        return replacement_string.sub(string_to_be_replaced, input_string)
