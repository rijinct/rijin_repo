from ig_api_methods import create_session, get_open_positions, get_historical_data, create_position
from email_alert import email_alert_sender
import sched, time
import warnings
import sys
print(sys.path)
#from azure.storage.blob import BlobClient
from ig_handler import difference_btw_values, check_existing_order, check_transaction_last_2_hrs
from ig_technical import calculate_macd, calculate_ST, calculate_pivot, calculate_vwap
import sys
from datetime import datetime

import logging
	
warnings.filterwarnings('ignore')
global ig_service,df

s = sched.scheduler(time.time, time.sleep)

logger = logging.getLogger('azure')
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)

logger.info("Demo")
#logger.DEBUG("Test")


def trigger_alert(superT, vwap, macd, pivot, positions, macd_prev_int, macd_prev_int_2, instr):
    diff = difference_btw_values(macd['macd'], macd['signal'])
    diff_prev_int = difference_btw_values(macd_prev_int['macd'],
                                          macd_prev_int['signal'])
    diff_prev_int_2 = difference_btw_values(macd_prev_int_2['macd'],
                                            macd_prev_int_2['signal'])
    logger.info('############# MACD PREV INTERVAL ########################')
    logger.info(macd_prev_int)
    logger.info('############# MACD ########################')
    logger.info(macd)
    logger.info('############ PIVOT ########################')
    logger.info(pivot)
    logger.info('############# DIFFERENCE ########################')
    logger.info(diff)
    logger.info('############# VWAP ########################')
    logger.info(vwap)
    logger.info('############# SuperT ########################')
    logger.info(superT)
    logger.info('#############################################')
    # diff = 0.4

    if (macd['Close'] > (pivot - 5)) & (
            (( macd['macd'] > macd['signal']) & (
                    macd_prev_int['macd'] < macd_prev_int['signal']) & (
                     diff < 2.5)) | (
                    (macd['macd'] < macd['signal']) & (
                    macd_prev_int['macd'] < macd_prev_int['signal']) & (
                            diff < 2.5) & (diff_prev_int > diff) & (diff_prev_int_2 > diff))):
        if (check_existing_order(positions, 'BUY')):
            if (check_transaction_last_2_hrs(instr)):
                if (((macd['Close'] < vwap) & (macd['Close'] > vwap - 7))|(((macd['Close'] > vwap) & (macd['Close'] < vwap + 7)))):
                    if(macd['Close'] > superT):
                        print("Checking Today is Friday")	
                        if (datetime.today().weekday() != 4)|(datetime.today().hour < 17):
                                print("Today is not Friday too")	
                                email_alert_sender("Creating Buy")							
                                logger.info('Creating a Buy position')
                                create_position(instr, 'BUY', 2)
        else:
            logger.info('Not Checking for Profit in BUY position, as stop loss & Profit is set')
            ##check_and_close_buy_positions(diff, macd, positions, macd_prev_int, diff_prev_int)

    if (check_existing_order(positions, 'BUY')):
        logger.info('No Buy Positions')
    else:
        if any(positions.direction == 'BUY'):
            logger.info('NOT Checking for Profit/Loss in BUY position as stop loss & Profit is set')
            ##check_and_close_buy_positions(diff, macd, positions, macd_prev_int,
            ##                             diff_prev_int)
    # Sell scenario
    if (macd['Close'] < (pivot + 5)) & (
            ((macd['macd'] < macd['signal']) & (
                    macd_prev_int['macd'] > macd_prev_int['signal']) & (
                     diff < 2.5)) | (
                    (macd['macd'] > macd['signal']) & (
                    macd_prev_int['macd'] > macd_prev_int['signal']) & (
                            diff < 2.5) & (diff_prev_int > diff) & (diff_prev_int > diff))):
        if (check_existing_order(positions, 'SELL')):
            if (check_transaction_last_2_hrs(instr)):
                if (((macd['Close'] > vwap) & (macd['Close'] < vwap + 7))|(((macd['Close'] < vwap) & (macd['Close'] < vwap - 7)))):
                    if(macd['Close'] > superT):
                      print("Checking Today is Friday")	
                      if (datetime.today().weekday() != 4)|(datetime.today().hour < 17):	
                        logger.info('Creating a SELL position')
                        email_alert_sender("Creating SELL")						
                        create_position(instr, 'SELL', 2)
        else:
            logger.info('Not Checking for Profit in SELL position as SL & Profit is set')
            ##check_and_close_sell_position(diff, macd, positions, macd_prev_int, diff_prev_int)

    if (check_existing_order(positions, 'SELL')):
        logger.info('No SELL Positions')
    else:
        if any(positions.direction == 'SELL'):
            logger.info('Not Checking for Profit/Loss in SELL position as SL & Profit is set')
            ##check_and_close_sell_position(diff, macd, positions, macd_prev_int, diff_prev_int)

def historical_data_fetch(epic,resolution,numpoints):
    df = None
    status = True
    try:
        df = get_historical_data(epic,resolution,numpoints)
        #df = get_historical_data('IX.D.ASX.IFD.IP', 'H', 100)
    except:
        df = get_historical_data(epic,resolution,numpoints)
        logger.info("Fetching hourly historical data failed. So skipping the run")
        #email_alert_sender("Failure Hour Prod")
        status = False
    return df, status

def execute():
    create_session()
    #results = ig_service.get_client_apps()
    timestr = time.strftime("%Y%m%d-%H%M%S")
    hour_file = 'hour-{}.csv'.format(timestr)
    macd_file = 'macd-{}.csv'.format(timestr)
    status = True
    # get_epic_names('ASX').to_csv('epic_list.csv')
    # logger.info(results)
    #logger.info("Starting positions")
    positions = get_open_positions()
    #logger.info("positions:{}".format(positions))

     
    # Hourly Handling due to holidays
    df_h,status = historical_data_fetch('IX.D.ASX.IFD.IP', 'H', 70)
    if not status: 
        df_h,status = historical_data_fetch('IX.D.ASX.IFD.IP', 'H', 80)
        if not status: 
            df_h,status = historical_data_fetch('IX.D.ASX.IFD.IP', 'H', 90)
            if not status:
                df_h,status = historical_data_fetch('IX.D.ASX.IFD.IP', 'H', 100)
                if not status:
                    df_h,status = historical_data_fetch('IX.D.ASX.IFD.IP', 'H', 120)
                    if not status:
                        df_h,status = historical_data_fetch('IX.D.ASX.IFD.IP', 'H', 140)
                        if not status:
                            df_h,status = historical_data_fetch('IX.D.ASX.IFD.IP', 'H', 160)
                            if not status:
                                df_h,status = historical_data_fetch('IX.D.ASX.IFD.IP', 'H', 180)
                                if not status:
                                    df_h,status = historical_data_fetch('IX.D.ASX.IFD.IP', 'H', 200)
    if not status: 
        email_alert_sender("Failure Hour Prod @ 70,80,90,100-200")

    df_d,status = historical_data_fetch('IX.D.ASX.IFD.IP', 'D', 2)
    if not status:
        df_d,status = historical_data_fetch('IX.D.ASX.IFD.IP', 'D', 4)

    if not status: 
        email_alert_sender("Failure Day Prod @ 2,4")
    
    if not status:
        logger.info("Issue in fetching hourly or day data")
    else:
        df_h.to_csv(hour_file)
        #df_h = pd.read_csv('hour_80.csv')
        # df_d = pd.read_csv('day.csv')
        pivot = calculate_pivot(df_d.iloc[0])
        # logger.info(calculate_macd(df_h))
        macd_calculated = calculate_macd(df_h)
        macd_calculated.to_csv(macd_file)
        macd = macd_calculated.iloc[-1]
        macd_prev_int = macd_calculated.iloc[-2]
        macd_prev_int_2 = macd_calculated.iloc[-3]
        vwap = calculate_vwap(df_h).iloc[-1]
        superT = calculate_ST(df_h)['SUPERT_10_3.0'].iloc[-1]
        #logger.info(superT)

        # pivot = 7245.3
        # positions = pd.read_csv('position.csv') #test
        trigger_alert(superT, vwap, macd, pivot, positions, macd_prev_int, macd_prev_int_2, 'IX.D.ASX.IFD.IP')



    ###s.enter(3600, 1, execute, (sc,))


if __name__ == "__main__":
	execute()
    ###s.enter(1, 1, execute, (s,))
    ###s.run()

	# Define parameters
	# connectionString = "DefaultEndpointsProtocol=https;AccountName=rijinstorageaccount;AccountKey=d7dC+ibJhNvaUIZU7jzCPpqhQh+kYpf89SKxUNK99J4hEph1vwlaANj8K/yqhO5ZaLwi/uTrAdey+AStD6fMFw==;EndpointSuffix=core.windows.net"
	# containerName = "ig-api-container"
	# outputBlobName	= "test.csv"

	# blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=outputBlobName)
	# dict = {'name':["aparna", "pankaj", "sudhir", "Geeku"], 
	# 		'degree': ["MBA", "BCA", "M.Tech", "MBA"], 
	# 		'score':[90, 40, 80, 98]} 
	
	# df.to_csv(outputBlobName, index = False)

	# with open(outputBlobName, "rb") as data:
	# 	blob.upload_blob(data)

		
