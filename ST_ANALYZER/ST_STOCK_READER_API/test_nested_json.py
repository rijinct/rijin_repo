'''
Created on 25-May-2020

@author: rithomas
'''
from pandas.io.json import json_normalize
from flatten_json import flatten
import DF_INPUT_READER_OPERATIONS.different_input_format_to_df as IN_FORMAT

data = {"tradedDate": "22MAY2020", "data": [{"pricebandupper": "167.10", "symbol": "SBIN", "applicableMargin": "35.12", "bcEndDate": "28-JUN-18", "totalSellQuantity": "-", "adhocMargin": "17.14", "companyName": "State Bank of India", "marketType": "N", "exDate": "15-JUN-18", "bcStartDate": "19-JUN-18", "css_status_desc": "Listed", "dayHigh": "155.60", "basePrice": "151.95", "securityVar": "12.82", "pricebandlower": "136.80", "sellQuantity5": "-", "sellQuantity4": "-", "sellQuantity3": "-", "cm_adj_high_dt": "18-JUL-19", "sellQuantity2": "-", "dayLow": "149.45", "sellQuantity1": "-", "quantityTraded": "8,70,69,955", "pChange": "-0.56", "totalTradedValue": "1,32,259.26", "deliveryToTradedQuantity": "24.40", "totalBuyQuantity": "21,999", "averagePrice": "151.90", "indexVar": "-", "cm_ffm": "57,889.94", "purpose": "ANNUAL GENERAL MEETING/ CHANGE IN REGISTRAR AND TRANSFER AGENT", "buyPrice2": "-", "secDate": "22-May-2020 00:00:00", "buyPrice1": "150.85", "high52": "373.80", "previousClose": "151.95", "ndEndDate": "-", "low52": "149.45", "buyPrice4": "-", "buyPrice3": "-", "recordDate": "-", "deliveryQuantity": "2,12,41,024", "buyPrice5": "-", "priceBand": "No Band", "extremeLossMargin": "5.16", "cm_adj_low_dt": "22-MAY-20", "varMargin": "12.82", "sellPrice1": "-", "sellPrice2": "-", "totalTradedVolume": "8,70,69,955", "sellPrice3": "-", "sellPrice4": "-", "sellPrice5": "-", "change": "-0.85", "surv_indicator": "-", "ndStartDate": "-", "buyQuantity4": "-", "isExDateFlag": False, "buyQuantity3": "-", "buyQuantity2": "-", "buyQuantity1": "21,999", "series": "EQ", "faceValue": "1.00", "buyQuantity5": "-", "closePrice": "150.85", "open": "152.00", "isinCode": "INE062A01020", "lastPrice": "151.10"}], "optLink": "/marketinfo/sym_map/symbolMapping.jsp?symbol=SBIN&instrument=-&date=-&segmentLink=17&symbolCount=2", "otherSeries": ["EQ", "N2", "N5", "N6"], "futLink": "/live_market/dynaContent/live_watch/get_quote/GetQuoteFO.jsp?underlying=SBIN&instrument=FUTSTK&expiry=28MAY2020&type=-&strike=-", "lastUpdateTime": "22-MAY-2020 16:00:00"}

unflat_json = flatten(data)
print(json_normalize(unflat_json).columns)

print(unflat_json)
print(IN_FORMAT.get_df_from_nested_json(data))
