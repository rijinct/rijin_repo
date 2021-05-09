'''
Created on 27-Mar-2020

@author: nageb
'''
import unittest

from healthmonitoring.framework.util.collection_util import \
    convert_list_dict_to_dict, get_lone_value, str2dict, DictObject, ListObject


class TestCollectionUtil(unittest.TestCase):

    def test_convert_list_dict_to_dict_given_empty_list(self):
        self.assertEqual(convert_list_dict_to_dict([]), {})

    def test_convert_list_dict_to_dict_given_list_of_single_dict(self):
        d = {"k": "v"}
        self.assertEqual(convert_list_dict_to_dict([d]), d)

    def test_convert_list_dict_to_dict_given_list_of_2_dicts(self):
        d1 = {"k1": "v1"}
        d2 = {"k2": "v2"}
        expected = {"k1": "v1", "k2": "v2"}
        self.assertEqual(convert_list_dict_to_dict([d1, d2]), expected)

    def test_get_lone_value_one_key(self):
        d = {"k": "v"}
        expected = "v"
        self.assertEqual(get_lone_value(d), expected)

    def test_get_value_of_lone_value_for_no_keys(self):
        self.assertRaises(ValueError, get_lone_value, dictionary={})

    def test_get_value_of_lone_value_with_excludes(self):
        d = {"k1": "v1", "k2": "v2", "k3": "v3"}
        self.assertEqual(get_lone_value(d, excludes=["k2", "k3"]), "v1")

    def test_get_value_of_lone_value_for_multiple_keys(self):
        d = {"k1": "v1", "k2": "v2", "k3": "v3"}
        self.assertRaises(ValueError,
                          get_lone_value,
                          dictionary=d,
                          excludes=["k2"])

    def test_str2dict_given_empty_string(self):
        self.assertEqual(str2dict(""), {})

    def test_str2dict_given_single_pair(self):
        self.assertEqual(str2dict("k:v"), {'k': 'v'})

    def test_str2dict_given_multiple_pairs(self):
        self.assertEqual(str2dict("a:x,b:y,c:z"), {
            'a': 'x',
            'b': 'y',
            'c': 'z'
        })


class TestDictObject(unittest.TestCase):

    def setUp(self):
        self._dict = {"k1": "v1", "k2": "v2"}

    def test_boolean_of_empty_dict_is_false(self):
        d = DictObject({})
        self.assertFalse(d)

    def test_boolean_of_non_empty_dict_is_true(self):
        d = DictObject(self._dict)
        self.assertTrue(d)

    def test_len(self):
        d = DictObject(self._dict)
        self.assertEqual(len(d), len(self._dict))

    def test_extraction_of_dict(self):
        d = DictObject(self._dict)
        self.assertDictEqual(d.dict, self._dict)

    def test_iterator_over_keys(self):
        d = DictObject(self._dict)
        self.assertListEqual(list(d), list(self._dict))

    def test_iterator_over_items(self):
        d = DictObject(self._dict)
        self.assertEqual(d.items(), self._dict.items())

    def test_keys(self):
        d = DictObject(self._dict)
        self.assertEqual(d.keys(), self._dict.keys())

    def test_values(self):
        d = DictObject(self._dict)
        self.assertListEqual(list(d.values()), list(self._dict.values()))

    def test_eq_with_object(self):
        d1 = DictObject(self._dict)
        d2 = DictObject({"k1": "v1", "k2": "v2"})
        self.assertEqual(d1, d2)

    def test_eq_with_dict(self):
        d1 = DictObject(self._dict)
        d2 = {"k1": "v1", "k2": "v2"}
        self.assertEqual(d1, d2)

    def test_getattr(self):
        d = DictObject(self._dict)
        self.assertEqual(d.k1, 'v1')
        self.assertEqual(d.k2, 'v2')

    def test_getattr_when_key_not_found(self):
        d = DictObject(self._dict)
        self.assertIsNone(d.k3)

    def test_getattr_for_nested_dict(self):
        d = DictObject({'outer_key': {"inner_key": "inner_value"}})
        self.assertEqual(d.outer_key.inner_key, "inner_value")


class TestListObject(unittest.TestCase):

    def setUp(self):
        self._list = ['a', 'b', 'c']

    def test_boolean_of_empty_list_is_false(self):
        obj = ListObject([])
        self.assertFalse(obj)

    def test_boolean_of_non_empty_list_is_true(self):
        obj = ListObject(self._list)
        self.assertTrue(obj)

    def test_len(self):
        obj = ListObject(self._list)
        self.assertEqual(len(obj), len(self._list))

    def test_extraction_of_dict(self):
        obj = ListObject(self._list)
        self.assertListEqual(obj.list, self._list)

    def test_iterator(self):
        obj = ListObject(self._list)
        self.assertEqual(tuple(obj), tuple(self._list))

    def test_eq_with_object(self):
        list1 = ListObject(self._list)
        list2 = ListObject(['a', 'b', 'c'])
        self.assertEqual(list1, list2)

    def test_eq_with_list(self):
        list1 = ListObject(self._list)
        list2 = ['a', 'b', 'c']
        self.assertEqual(list1, list2)

    def test_get_index(self):
        obj = ListObject(self._list)
        self.assertEqual(obj[0], 'a')
        self.assertEqual(obj[1], 'b')
        self.assertEqual(obj[-1], 'c')

    def test_get_index_when_index_invalid(self):
        obj = ListObject(self._list)
        self.assertRaises(IndexError, obj.__getitem__, index=9)

    def test_getattr_for_nested_list(self):
        obj = ListObject([[1, 2, 3], [4, 5, 6]])
        self.assertEqual(obj[0][1], 2)
        self.assertEqual(obj[1][1], 5)
        self.assertEqual(obj[1][2], 6)


class classTestDictObjectListObjectConversions(unittest.TestCase):

    def test(self):
        ds = [{'k1': [[{'k2': {'k3': [9]}}]]}]
        obj = ListObject(ds)
        self.assertEqual(obj[0].k1[0][0].k2.k3[0], 9)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
