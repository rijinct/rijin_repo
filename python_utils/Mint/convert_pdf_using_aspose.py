
# Import Module
import pdftables_api
  
# API KEY VERIFICATION
conversion = pdftables_api.Client('6zu8iio1fx3l')
  
# PDf to Excel 
# (Hello.pdf, Hello)
conversion.xlsx("test.pdf", "test.xlsx")