from html import HTML

class HtmlUtil:

        def generateHtmlFromList(self, label, outputList):
                self.h = HTML()
                self.h.p(label, style="font-size: large; font-style: bold")
                table = self.h.table(cellpadding="10", style="font-family: arial, sans-serif; border-collapse: collapse; width: 100%;")
                header = outputList[0]
                headerRow = table.tr
                for field in header.split(','):
                        headerRow.th(field, style="border: 1px solid #dddddd; color: white; padding: 8px;", bgcolor="#2A1111")
                values = outputList[1:]
                for value in values:
                        row = table.tr
                        for item in value.split(','):
				if item.endswith('Red'):
					item = item[:-3]
					row.td(str(item), style="border: 1px solid #dddddd; text-align: justify; padding: 8px; font-family: arial, sans-serif;", bgcolor="#FF0000")
				elif item.endswith('Orange'):
					item = item[:-6]
					row.td(str(item), style="border: 1px solid #dddddd; text-align: justify; padding: 8px; font-family: arial, sans-serif;", bgcolor="#FF8000")
				else:	row.td(str(item), style="border: 1px solid #dddddd; text-align: justify; padding: 8px; font-family: arial, sans-serif;")
		return self.h

	def generateHtmlFromDictList(self, label, dictList):
		outputList = []
		outputList.append(','.join(dictList[0].keys()))
		values = dictList
		for value in values:
			outputList.append(','.join(value.values()))
		return self.generateHtmlFromList(label, outputList)
