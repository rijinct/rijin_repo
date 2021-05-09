'''
Created on 11-Jun-2020

@author: deerakum
'''


class HtmlUtil:
    def generateHtmlFromList(self, label, outputList):
        label_html = '''<p style="font-size: medium; font-style: bold">{0}</p>\n'''.format(# noqa: E501
            label)
        table_html = '''<table cellpadding="10" style="font-family: arial, sans-serif; border-collapse: collapse; width: 80%;">\n'''  # noqa: E501
        header = outputList[0]
        table_html += '''<tr>'''
        for field in header.split(','):
            table_html += '''<th style="border: 1px solid #dddddd; color: white; padding: 8px; font-size: 12px;" bgcolor="#2A1111">{0}</th>'''.format(  # noqa: E501
                field)
        table_html += '''</tr>\n'''
        values = outputList[1:]
        for value in values:
            table_html += '''<tr>'''
            for item in value.split(','):
                if item.endswith('Red'):
                    item = item[:-3]
                    table_html += '''<td style="border: 1px solid #dddddd; text-align: justify; padding: 8px; font-family: arial, sans-serif; font-size: 12px;" bgcolor="#FF0000"">{0}</td>\n'''.format(  # noqa: E501
                        str(item))
                elif item.endswith('Orange'):
                    item = item[:-6]
                    table_html += '''<td style="border: 1px solid #dddddd; text-align: justify; padding: 8px; font-family: arial, sans-serif; font-size: 12px;" bgcolor="#FF8000">{0}</td>\n'''.format(  # noqa: E501
                        str(item))
                elif item.endswith('Green'):
                    item = item[:-5]
                    table_html += '''<td style="border: 1px solid #dddddd; text-align: justify; padding: 8px; font-family: arial, sans-serif; font-size: 12px;" bgcolor="#006400">{0}</td>\n'''.format(  # noqa: E501
                        str(item))
                else:
                    table_html += '''<td style="border: 1px solid #dddddd; text-align: justify; padding: 8px; font-family: arial, sans-serif; font-size: 12px;">{0}</td>\n'''.format(  # noqa: E501
                        str(item))
        table_html += '''</tr>\n</table>'''
        return label_html + table_html

    def generateHtmlFromDictList(self, label, dictList):
        outputList = []
        outputList.append(','.join(list(dictList[0].keys())))
        values = dictList
        for value in values:
            outputList.append(','.join(list(value.values())))
        return self.generateHtmlFromList(label, outputList)
