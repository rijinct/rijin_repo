macd = 23.327832
signal = 27.878915
histogram = 5.448917
Close = 7368.200000
pivot = 7273
diff = 2

if (Close > pivot) & (((macd > signal) & (diff < 1)) | ((macd < signal) & (diff < 1))):
    print("create")
