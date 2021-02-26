import unittest
import sys
sys.path.append('framework/')
from framework.dynamic_dimension_mapper import DynamicDimensionMapper  # noqa E402
from framework.util import get_dynamic_dimension_views  # noqa E402


class TestDynamicDimensionMapper(unittest.TestCase):
    def setUp(self):
        views = get_dynamic_dimension_views(
            "hdfs://projectcluster/ngdb/es/DYN_DIM_1/LOC_DIM_EXTENSION_1")
        mapper = DynamicDimensionMapper()
        columns = [
            'ref_imsi', 'l_dim_1', 'l_dim_2', 'l_dim_3', 'l_dim_id', 'from_dt',
            'to_dt'
        ]
        self.__kvmap = mapper.generate(views, columns)

    def test_mapSize(self):
        self.assertEqual(38, len(self.__kvmap.keys()))

    def test_mapViewKeyValue(self):
        self.assertEqual('EXT_LOC_DIM_EXTENSION_1_VIEW',
                         self.__kvmap['VIEW_EXT_DIM_EXTENSION'])
        self.assertEqual('ES_LOC_DIM_EXTENSION_1_VIEW',
                         self.__kvmap['VIEW_ES_DIM_EXTENSION'])
        self.assertEqual('ES_LOC_DIM_ID_1_VIEW',
                         self.__kvmap['VIEW_ES_DIM_ID'])
        self.assertEqual('ES_EXT_DIM_EXTENSION_VIEW',
                         self.__kvmap['VIEW_ES_EXT_DIM_EXTENSION'])

    def test_mapColumnKeyValue(self):
        self.assertEqual(self.__kvmap['COL_REFCOL'], 'ref_imsi')
        self.assertEqual(self.__kvmap['COL_EXTREFCOL'], 'ext_ref_imsi')
        self.assertEqual(self.__kvmap['COL_REFCOL_ALIAS_EXTREFCOL'],
                         'ref_imsi as ext_ref_imsi')
        self.assertEqual(self.__kvmap['COL_EXTREFCOL_ALIAS_REFCOL'],
                         'ext_ref_imsi as ref_imsi')

        self.assertEqual(self.__kvmap['COL_DIMID'], 'l_dim_id')
        self.assertEqual(self.__kvmap['COL_EXTDIMID'], 'ext_l_dim_id')
        self.assertEqual(self.__kvmap['COL_DIMID_ALIAS_EXTDIMID'],
                         'l_dim_id as ext_l_dim_id')
        self.assertEqual(self.__kvmap['COL_EXTDIMID_ALIAS_DIMID'],
                         'ext_l_dim_id as l_dim_id')

        self.assertEqual(self.__kvmap['COL_FROMDT'], 'from_dt')
        self.assertEqual(self.__kvmap['COL_EXTFROMDT'], 'ext_from_dt')
        self.assertEqual(self.__kvmap['COL_FROMDT_ALIAS_EXTFROMDT'],
                         'from_dt as ext_from_dt')
        self.assertEqual(self.__kvmap['COL_EXTFROMDT_ALIAS_FROMDT'],
                         'ext_from_dt as from_dt')

        self.assertEqual(self.__kvmap['COL_TODT'], 'to_dt')
        self.assertEqual(self.__kvmap['COL_EXTTODT'], 'ext_to_dt')
        self.assertEqual(self.__kvmap['COL_TODT_ALIAS_EXTTODT'],
                         'to_dt as ext_to_dt')
        self.assertEqual(self.__kvmap['COL_EXTTODT_ALIAS_TODT'],
                         'ext_to_dt as to_dt')

    def test_mapColumnsKeyValue(self):
        self.assertEqual(self.__kvmap['COL_DIMCOLS_DEFAULT'], "'NA','NA','NA'")
        self.assertEqual(self.__kvmap['COL_DIMCOLS'],
                         "l_dim_1,l_dim_2,l_dim_3")
        self.assertEqual(self.__kvmap['COL_EXTDIMCOLS'],
                         "ext_l_dim_1,ext_l_dim_2,ext_l_dim_3")
        self.assertEqual(
            self.__kvmap['COL_DIMCOLS_ALIAS_EXTDIMCOLS'],
            "l_dim_1 as ext_l_dim_1," + "l_dim_2 as ext_l_dim_2," +
            "l_dim_3 as ext_l_dim_3")
        self.assertEqual(
            self.__kvmap['COL_EXTDIMCOLS_ALIAS_DIMCOLS'],
            "ext_l_dim_1 as l_dim_1," + "ext_l_dim_2 as l_dim_2," +
            "ext_l_dim_3 as l_dim_3")
        self.assertEqual(
            self.__kvmap['COL_DIMCOLS_NVL_DEFAULT'],
            "case when l_dim_1 = 'null' then 'NA' else l_dim_1 end as l_dim_1,"
            +
            "case when l_dim_2 = 'null' then 'NA' else l_dim_2 end as l_dim_2,"
            +
            "case when l_dim_3 = 'null' then 'NA' else l_dim_3 end as l_dim_3")

    def test__mapConditionKeyValue(self):
        self.assertEqual(
            self.__kvmap['COND_OR_NOTEQUAL_DIMCOLS_a_b'],
            "(a.l_dim_1 <> b.ext_l_dim_1) or " +
            "(a.l_dim_2 <> b.ext_l_dim_2) or " +
            "(a.l_dim_3 <> b.ext_l_dim_3)")
        self.assertEqual(
            self.__kvmap['COND_AND_EQUAL_DIMCOLS_a_b'],
            "(a.l_dim_1 = b.ext_l_dim_1) and " +
            "(a.l_dim_2 = b.ext_l_dim_2) and " + "(a.l_dim_3 = b.ext_l_dim_3)")


if __name__ == "__main__":
    unittest.main()
