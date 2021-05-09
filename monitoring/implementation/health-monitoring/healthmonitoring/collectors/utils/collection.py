'''
Created on 04-May-2020

@author: a4yadav
'''


class CollectionUtil:

    @staticmethod
    def get_list_without_null_values(data, seperator=' '):
        return list(filter(None, data.split(seperator)))

    @staticmethod
    def frame_dict_using_lists(keys, values):
        result = {}
        for key, value in zip(keys, values):
            result[key] = value
        return result

    @staticmethod
    def map_items_to_int(data):
        return list(map(int, data))
