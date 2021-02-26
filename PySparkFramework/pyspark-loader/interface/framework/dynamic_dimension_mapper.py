import re


class DynamicDimensionConstants:
    QUERY_GET_DEFAULT_DIMID = 'QUERY_GET_DEFAULT_DIMID'
    QUERY_ADD_DEFAULT_DIMID = 'QUERY_ADD_DEFAULT_DIMID'
    QUERY_GET_CSV_DUPLICATE_COUNT = 'QUERY_GET_CSV_DUPLICATE_COUNT'
    QUERY_GET_CSV_DUPLICATE = 'QUERY_GET_CSV_DUPLICATE'
    QUERY_GET_CSV_COLUMNS_ROWNO = 'QUERY_GET_CSV_COLUMNS_ROWNO'
    QUERY_GET_CSV_COLUMNS = 'QUERY_GET_CSV_COLUMNS'
    QUERY_GET_ES_EXT_DIM_EXTENSION = 'QUERY_GET_ES_EXT_DIM_EXTENSION'
    QUERY_GET_ES_DIM_ID_COLUMNS = 'QUERY_GET_ES_DIM_ID_COLUMNS'
    QUERY_GET_ES_DIM_EXTEN_LATEST_COL = 'QUERY_GET_ES_DIM_EXTEN_LATEST_COL'
    QUERY_GET_ES_DIM_EXTEN_HISTORY_COL = 'QUERY_GET_ES_DIM_EXTEN_HISTORY_COL'
    VIEW_ES_DIM_ID = 'VIEW_ES_DIM_ID'
    VIEW_ES_DIM_EXTENSION = 'VIEW_ES_DIM_EXTENSION'
    VIEW_EXT_DIM_EXTENSION = 'VIEW_EXT_DIM_EXTENSION'
    VIEW_ES_EXT_DIM_EXTENSION = 'VIEW_ES_EXT_DIM_EXTENSION'

    ADDNL_PATH = "/file_load_info=latest"
    TEMP_LATEST_PATH = '{}/file_load_info=latest1/'
    REMOVE_TEMP_LATEST_FILE = 'hdfs dfs -rm {0}/file_load_info=latest1/*'
    REMOVE_LATEST_FILE = 'hdfs dfs -rm {0}/file_load_info=latest/*'
    REMOVE_LATEST_DIR = 'hdfs dfs -rmdir {0}/file_load_info=latest'
    MOVE_LATEST = 'hdfs dfs -mv {0}/file_load_info=latest1 {0}/file_load_info=latest'  # noqa: E501

    HISTORY_PATH = '{0}/file_load_info={1}/'
    MOVE_HISTORY = 'hdfs dfs -mv {0}/file_load_info={1}/* {0}/file_load_info={2}/'  # noqa: E501

    DROP_VIEW = 'DROP VIEW {}'
    REFRESH_TABLE = 'REFRESH table {0}'
    MSCK_REPAIR_TABLE = 'msck repair table {0}'
    TRUNCATE_TABLE = 'truncate table {0}.{1}'
    SHOW_PARTITIONS = 'show partitions {}'
    DROP_PARTITION = 'ALTER TABLE {0} DROP IF EXISTS PARTITION(file_load_info={1})'  # noqa: E501

    REMOVE_CSV_FILE = 'rm /mnt/staging/import/DYNAMIC_DIMENSION_1/*/*.processing'  # noqa: E501


class DynamicDimensionMapper:
    def __init__(self, column_names=[]):
        self.__dim_dict = {}
        self.__dim_cols = []
        self.__ext_dim_cols = []
        self.__dim_cols_alias = []
        self.__ext_dim_cols_alias = []
        self.__dim_cols_default = []
        self.__dim_cols_nvl_default = []
        self.__equal_condition_dim_cols = []
        self.__not_equal_condition_dim_cols = []

    def generate(self, views, columns):
        self.__map_view_key(views)
        self.__map_column_key(columns)
        self.__map_columns_key()
        self.__map_condition_key()
        self.__map_query_key()
        return self.__dim_dict

    def __map_view_key(self, views):
        self.__dim_dict[
            DynamicDimensionConstants.VIEW_EXT_DIM_EXTENSION] = views[
                DynamicDimensionConstants.VIEW_EXT_DIM_EXTENSION]
        self.__dim_dict[
            DynamicDimensionConstants.VIEW_ES_DIM_EXTENSION] = views[
                DynamicDimensionConstants.VIEW_ES_DIM_EXTENSION]
        self.__dim_dict[DynamicDimensionConstants.VIEW_ES_DIM_ID] = views[
            DynamicDimensionConstants.VIEW_ES_DIM_ID]
        self.__dim_dict[
            DynamicDimensionConstants.
            VIEW_ES_EXT_DIM_EXTENSION] = "ES_EXT_DIM_EXTENSION_VIEW"

    def __map_column_key(self, columns):
        for col in columns:
            col = col.lower()
            if col.startswith('ref_'):
                self.__add_key_value("REFCOL", col)
            elif re.search(r"_dim_\d", col) is not None:
                self.__append_values(col)
            elif col.endswith('_dim_id'):
                self.__add_key_value("DIMID", col)
            else:
                self.__add_key_value(col.upper().replace("_", ""), col)

    def __add_key_value(self, ckey, cvalue):
        self.__dim_dict["COL_{}".format(ckey)] = cvalue
        self.__dim_dict["COL_EXT{}".format(ckey)] = "ext_{}".format(cvalue)
        self.__dim_dict["COL_{0}_ALIAS_EXT{0}".format(
            ckey)] = "{0} as ext_{0}".format(cvalue)
        self.__dim_dict["COL_EXT{0}_ALIAS_{0}".format(
            ckey)] = "ext_{0} as {0}".format(cvalue)

    def __append_values(self, cvalue):
        self.__dim_cols.append(cvalue)
        self.__dim_cols_default.append("'NA'")
        self.__ext_dim_cols.append("ext_{}".format(cvalue))
        self.__dim_cols_alias.append("{0} as ext_{0}".format(cvalue))
        self.__ext_dim_cols_alias.append("ext_{0} as {0}".format(cvalue))
        self.__dim_cols_nvl_default.append(
            "case when {0} = 'null' then 'NA' else {0} end as {0}".format(
                cvalue))
        self.__equal_condition_dim_cols.append(
            "(a.{0} = b.ext_{0})".format(cvalue))
        self.__not_equal_condition_dim_cols.append(
            "(a.{0} <> b.ext_{0})".format(cvalue))

    def __map_columns_key(self):
        self.__dim_dict["COL_DIMCOLS_DEFAULT"] = ",".join(
            self.__dim_cols_default)
        self.__dim_dict["COL_DIMCOLS"] = ",".join(self.__dim_cols)
        self.__dim_dict["COL_EXTDIMCOLS"] = ",".join(self.__ext_dim_cols)
        self.__dim_dict["COL_DIMCOLS_ALIAS_EXTDIMCOLS"] = ",".join(
            self.__dim_cols_alias)
        self.__dim_dict["COL_EXTDIMCOLS_ALIAS_DIMCOLS"] = ",".join(
            self.__ext_dim_cols_alias)
        self.__dim_dict["COL_DIMCOLS_NVL_DEFAULT"] = ",".join(
            self.__dim_cols_nvl_default)

    def __map_condition_key(self):
        self.__dim_dict["COND_OR_NOTEQUAL_DIMCOLS_a_b"] = " or ".join(
            self.__not_equal_condition_dim_cols)
        self.__dim_dict["COND_AND_EQUAL_DIMCOLS_a_b"] = " and ".join(
            self.__equal_condition_dim_cols)

    def __map_query_key(self):
        get_default_dimid = '''select * from {0} where #COL_DIMID# = -88 '''
        add_default_dimid = ''' insert into {0}
         values(-88,#COL_DIMCOLS_DEFAULT#)'''
        get_csv_duplicate_count = ''' select count(*),#COL_REFCOL# from
          {0}.{1} group by #COL_REFCOL# having count(*) >1'''
        get_csv_duplicate = '''SELECT #COL_REFCOL#,#COL_DIMCOLS# FROM
         (SELECT #COL_REFCOL#,#COL_DIMCOLS#,
          ROW_NUMBER() OVER (PARTITION BY #COL_REFCOL# ORDER BY #COL_REFCOL#)
           ROWPARTITION FROM {0}.{1})ROW_TABLE
            WHERE ROW_TABLE.ROWPARTITION>1'''
        get_csv_colums_rowno = '''SELECT upper(#COL_REFCOL#) as #COL_REFCOL#,
         #COL_DIMCOLS_NVL_DEFAULT# FROM (SELECT #COL_REFCOL#,#COL_DIMCOLS#,
          ROW_NUMBER() OVER (PARTITION BY #COL_REFCOL# ORDER BY #COL_REFCOL#)
           ROWPARTITION FROM {0}.{1})ROW_TABLE
            WHERE ROW_TABLE.ROWPARTITION=1'''
        get_csv_colums = '''select upper(#COL_REFCOL#) as #COL_REFCOL#,
         #COL_DIMCOLS_NVL_DEFAULT# from {0}.{1}'''
        get_es_ext_dim_extension = '''select *,
         case when a.#COL_REFCOL# is null then 'A'
             when b.#COL_EXTREFCOL# is null then 'B'
              when (#COND_OR_NOTEQUAL_DIMCOLS_a_b#) = true then 'C'
               else 'I' end as flag
                from
                 (select
                  * from #VIEW_ES_DIM_EXTENSION# where #COL_TODT# is null) a
                 full outer join
                  (select
                   #COL_REFCOL_ALIAS_EXTREFCOL#,#COL_DIMID_ALIAS_EXTDIMID#,
                   #COL_DIMCOLS_ALIAS_EXTDIMCOLS#,#COL_FROMDT_ALIAS_EXTFROMDT#,
                   #COL_TODT_ALIAS_EXTTODT# from #VIEW_EXT_DIM_EXTENSION#)b
                   on (a.#COL_REFCOL# = b.#COL_EXTREFCOL#)'''
        get_es_dim_id_columns = '''select * from
         (select max(#COL_DIMID#) #COL_DIMID#,#COL_DIMCOLS#
          from
           ( select #COL_EXTDIMID_ALIAS_DIMID#,#COL_EXTDIMCOLS_ALIAS_DIMCOLS#
            from #VIEW_ES_EXT_DIM_EXTENSION#
             where flag in ('A','C')
              group by #COL_EXTDIMID#, #COL_EXTDIMCOLS#
               union all
                select #COL_DIMID#,#COL_DIMCOLS# from #VIEW_ES_DIM_ID#
                 group by #COL_DIMID#,#COL_DIMCOLS# ) group by #COL_DIMCOLS#)
                  where #COL_DIMID# is null'''
        get_es_dim_exten_latest_col = '''select #COL_REFCOL#,
         #COL_EXTDIMID_ALIAS_DIMID#, #COL_DIMCOLS#,#COL_FROMDT#,#COL_TODT#
         from(
          select #COL_EXTREFCOL_ALIAS_REFCOL#,#COL_EXTDIMID_ALIAS_DIMID#,
          #COL_EXTDIMCOLS_ALIAS_DIMCOLS#,#COL_EXTFROMDT_ALIAS_FROMDT#,
          #COL_EXTTODT_ALIAS_TODT#
           from #VIEW_ES_EXT_DIM_EXTENSION#
            where flag in('A','C')
             union all
              select #COL_REFCOL#,#COL_DIMID#,#COL_DIMCOLS#,
              #COL_FROMDT#,#COL_TODT#
               from #VIEW_ES_EXT_DIM_EXTENSION#
                where flag in('B','I')) a
                 left outer join
                  (select #COL_DIMID_ALIAS_EXTDIMID#,
                  #COL_DIMCOLS_ALIAS_EXTDIMCOLS#
                   from {0}) b
                    on (#COND_AND_EQUAL_DIMCOLS_a_b#)'''
        get_es_dim_exten_hist_col = '''select #COL_REFCOL#,
         #COL_DIMID#,#COL_DIMCOLS#,#COL_FROMDT#,
          (#COL_EXTFROMDT# - 1000) as #COL_TODT#
           from #VIEW_ES_EXT_DIM_EXTENSION# where flag ='C' '''
        tag = '#{}#'

        for x in self.__dim_dict:
            get_default_dimid = get_default_dimid.replace(
                tag.format(x), self.__dim_dict[x])
            add_default_dimid = add_default_dimid.replace(
                tag.format(x), self.__dim_dict[x])
            get_csv_duplicate_count = get_csv_duplicate_count.replace(
                tag.format(x), self.__dim_dict[x])
            get_csv_duplicate = get_csv_duplicate.replace(
                tag.format(x), self.__dim_dict[x])
            get_csv_colums_rowno = get_csv_colums_rowno.replace(
                tag.format(x), self.__dim_dict[x])
            get_csv_colums = get_csv_colums.replace(tag.format(x),
                                                    self.__dim_dict[x])
            get_es_ext_dim_extension = get_es_ext_dim_extension.replace(
                tag.format(x), self.__dim_dict[x])
            get_es_dim_id_columns = get_es_dim_id_columns.replace(
                tag.format(x), self.__dim_dict[x])
            get_es_dim_exten_latest_col = get_es_dim_exten_latest_col.replace(
                tag.format(x), self.__dim_dict[x])
            get_es_dim_exten_hist_col = get_es_dim_exten_hist_col.replace(
                tag.format(x), self.__dim_dict[x])

        self.__dim_dict[DynamicDimensionConstants.
                        QUERY_GET_DEFAULT_DIMID] = get_default_dimid
        self.__dim_dict[DynamicDimensionConstants.
                        QUERY_ADD_DEFAULT_DIMID] = add_default_dimid
        self.__dim_dict[
            DynamicDimensionConstants.
            QUERY_GET_CSV_DUPLICATE_COUNT] = get_csv_duplicate_count
        self.__dim_dict[DynamicDimensionConstants.
                        QUERY_GET_CSV_DUPLICATE] = get_csv_duplicate
        self.__dim_dict[DynamicDimensionConstants.
                        QUERY_GET_CSV_COLUMNS_ROWNO] = get_csv_colums_rowno
        self.__dim_dict[
            DynamicDimensionConstants.QUERY_GET_CSV_COLUMNS] = get_csv_colums
        self.__dim_dict[
            DynamicDimensionConstants.
            QUERY_GET_ES_EXT_DIM_EXTENSION] = get_es_ext_dim_extension
        self.__dim_dict[DynamicDimensionConstants.
                        QUERY_GET_ES_DIM_ID_COLUMNS] = get_es_dim_id_columns
        self.__dim_dict[
            DynamicDimensionConstants.
            QUERY_GET_ES_DIM_EXTEN_LATEST_COL] = get_es_dim_exten_latest_col
        self.__dim_dict[
            DynamicDimensionConstants.
            QUERY_GET_ES_DIM_EXTEN_HISTORY_COL] = get_es_dim_exten_hist_col
