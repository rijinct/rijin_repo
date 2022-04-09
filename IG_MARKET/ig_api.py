from trading_ig import IGService
from config import config
from datetime import datetime
import pandas_ta as ta
import pandas as pd
import sched, time

s = sched.scheduler(time.time, time.sleep)


def create_session():
    global ig_service
    print(config.username)
    ig_service = IGService(config.username, config.password, config.api_key,
                           config.acc_type)
    ig_service.create_session()


def get_open_positions():
    open_positions = ig_service.fetch_open_positions()
    return open_positions


def get_historical_data(epic, resolution, num_points):
    response = ig_service.fetch_historical_prices_by_epic_and_num_points(epic,
                                                                         resolution,
                                                                         num_points)
    df_ask = response['prices']['ask']
    return df_ask


def get_epic_names(pattern):
    results = ig_service.search_markets(pattern)
    return results[['epic', 'instrumentName']]


def create_position(epicVal, dir, size):
    resp = ig_service.create_open_position(
        currency_code='AUD',
        direction=dir,
        epic=epicVal,
        order_type='MARKET',
        expiry='-',
        force_open='true',
        guaranteed_stop='false',
        size=size,
        level=None,
        limit_distance=5,
        limit_level=None,
        quote_id=None,
        stop_level=None,
        stop_distance=30,
        trailing_stop=None,
        trailing_stop_increment=None)
    resp


def close_position(dealId, directionVal, epic, expiry, level, order_type,
                   quote_id, size):
    #print(dealId)
    resp = ig_service.close_open_position(
        deal_id=dealId,
        direction=directionVal,
        epic=epic,
        expiry='-',
        level=level,
        order_type=order_type,
        quote_id=quote_id,
        size=size)
    resp


def calculate_macd(df):
    df.ta.macd(close='Close', fast=12, slow=26, signal=9, append=True)
    # print(df.columns)
    df.columns = ['Open', 'High', 'Low', 'Close', 'macd', 'histogram',
               'signal']  # uncomment
    #df.columns = ['Date','Open', 'High', 'Low', 'Close', 'macd', 'histogram','signal']  #test
    pd.set_option("display.max_columns", None)  # show all columns
    return df[['macd', 'signal', 'histogram', 'Close']]


def calculate_pivot(df):
    last_day = df
    last_day['Pivot'] = (last_day['High'] + last_day['Low'] + last_day[
        'Close']) / 3
    last_day['R1'] = 2 * last_day['Pivot'] - last_day['Low']
    last_day['S1'] = 2 * last_day['Pivot'] - last_day['High']
    last_day['R2'] = last_day['Pivot'] + (last_day['High'] - last_day['Low'])
    last_day['S2'] = last_day['Pivot'] - (last_day['High'] - last_day['Low'])
    last_day['R3'] = last_day['Pivot'] + 2 * (
            last_day['High'] - last_day['Low'])
    last_day['S3'] = last_day['Pivot'] - 2 * (
            last_day['High'] - last_day['Low'])
    return last_day['Pivot']


def difference_btw_values(val1, val2):
    h_val = ''
    l_val = ''
    print(val1)
    print(val2)
    if val1 > val2:
        h_val = val1
        l_val = val2
    else:
        h_val = val2
        l_val = val1
    return h_val - l_val


def check_existing_order(positions, direction):
    #print(positions['direction'])
    if (positions.empty):
        return True
    else:
        if any(positions.direction == 'BUY'):
            print("Already Buy exists")
            return False
        if any(positions.direction == 'SELL'):
            print("Already Sell exists")
            return False

def check_transaction_last_2_hrs(instr):
    transaction = ig_service.fetch_account_activity_by_period(21600000)
    if any(transaction.epic == instr):
        print('Already transacted in the last 3 hrs')
        return False
    else:
        print('No trasaction in the last 3 hrs')
        return True


def trigger_alert(macd, pivot, positions, macd_prev_int, instr):
    diff = difference_btw_values(macd['macd'], macd['signal'])
    diff_prev_int = difference_btw_values(macd_prev_int['macd'],
                                          macd_prev_int['signal'])
    print('############# MACD PREV INTERVAL ########################')
    print(macd_prev_int)
    print('############# MACD ########################')
    print(macd)
    print('############ PIVOT ########################')
    print(pivot)
    print('############# DIFFERENCE ########################')
    print(diff)
    # diff = 0.4
    if (macd['Close'] > (pivot - 5)) & (
            ((macd['macd'] > macd['signal']) & (
                    macd_prev_int['macd'] < macd_prev_int['signal']) & (
                     diff < 2.5)) | (
                    (macd['macd'] < macd['signal']) & (
                    macd_prev_int['macd'] < macd_prev_int['signal']) & (
                            diff < 2.5) & (diff_prev_int > diff))):
        if (check_existing_order(positions, 'BUY')):
            if (check_transaction_last_2_hrs(instr)):
                print('Creating a Buy position')
                create_position(instr, 'BUY', 1)
        else:
            print('Not Checking for Profit in BUY position, as stop loss & Profit is set')
            ##check_and_close_buy_positions(diff, macd, positions, macd_prev_int, diff_prev_int)

    if (check_existing_order(positions, 'BUY')):
        print('No Buy Positions')
    else:
        if any(positions.direction == 'BUY'):
            print('NOT Checking for Profit/Loss in BUY position as stop loss & Profit is set')
            ##check_and_close_buy_positions(diff, macd, positions, macd_prev_int,
             ##                             diff_prev_int)
    # Sell scenario
    if (macd['Close'] < (pivot + 5)) & (
            ((macd['macd'] < macd['signal']) & (
                    macd_prev_int['macd'] > macd_prev_int['signal']) & (
                     diff < 2.5)) | (
                    (macd['macd'] > macd['signal']) & (
                    macd_prev_int['macd'] > macd_prev_int['signal']) & (
                            diff < 2.5) & (diff_prev_int > diff))):
        if (check_existing_order(positions, 'SELL')):
            if (check_transaction_last_2_hrs(instr)):
                print('Creating a SELL position')
                create_position(instr, 'SELL', 1)
        else:
            print('Not Checking for Profit in SELL position as SL & Profit is set')
            ##check_and_close_sell_position(diff, macd, positions, macd_prev_int, diff_prev_int)

    if (check_existing_order(positions, 'SELL')):
        print('No SELL Positions')
    else:
        if any(positions.direction == 'SELL'):
            print('Not Checking for Profit/Loss in SELL position as SL & Profit is set')
            ##check_and_close_sell_position(diff, macd, positions, macd_prev_int, diff_prev_int)


def check_and_close_sell_position(diff, macd, positions, macd_prev_int, diff_prev_int):
    target_profit = positions['level'].iloc[0] - 5
    #stop_loss = positions['level'].iloc[0] + 20
    if (macd['Close'] < target_profit):
        print('target hit, closing deal')
        close_position(positions['dealId'].iloc[0], 'BUY', None, None,
                       None, 'MARKET', None, 1)
    ###crossOverScenario
    print('checking for stopLoss')
    if ((macd['macd'] > macd['signal']) & (diff > 1)) & (
            (macd_prev_int['macd'] < macd_prev_int['signal']) & (
            diff_prev_int < 1)):
        print('StopLoss hit, closing SELL position')
        # close_position('DIAAAAHW4F6P7AZ','SELL',None,None,None,'MARKET',None,1)
        close_position(positions['dealId'].iloc[0], 'BUY', None, None,
                       None, 'MARKET', None, 1)
    ###False crossover scenario
    if ((macd['macd'] > macd['signal']) & (diff > 0.6) & (diff_prev_int < diff)):
        print('StopLoss hit, closing SELL position')
        # close_position('DIAAAAHW4F6P7AZ','SELL',None,None,None,'MARKET',None,1)
        close_position(positions['dealId'].iloc[0], 'BUY', None, None,
                       None, 'MARKET', None, 1)

    #Hitting -500$ stop loss, actually can be skipped as create order has a stopLoss set
    # if (macd['Close'] > stop_loss):
    #     print('StopLoss hit because of -500$, closing BUY position')
    #     close_position(positions['dealId'].iloc[0], 'BUY', None, None,
    #                    None, 'MARKET', None, 1)


def check_and_close_buy_positions(diff, macd, positions, macd_prev_int,
                                  diff_prev_int):
    target_profit = positions['level'].iloc[0] + 5
    #stop_loss = positions['level'].iloc[0] - 20
    if (macd['Close'] > target_profit):
        print('target hit, closing deal')
        close_position(positions['dealId'].iloc[0], 'SELL', None, None,
                       None, 'MARKET', None, 1)
    print('checking for stopLoss')
    ###crossOverScenario
    if ((macd['macd'] < macd['signal']) & (diff < 1)) & (
            (macd_prev_int['macd'] > macd_prev_int['signal']) & (
            diff_prev_int < 1)):
        print('StopLoss hit, closing BUY position')
        # close_position('DIAAAAHW4F6P7AZ','SELL',None,None,None,'MARKET',None,1)
        close_position(positions['dealId'].iloc[0], 'SELL', None, None,
                       None, 'MARKET', None, 1)
    ###False crossover scenario
    if ((macd['macd'] < macd['signal']) & (diff > 0.6) & (diff_prev_int < diff)):
        print('StopLoss hit, closing BUY position')
        # close_position('DIAAAAHW4F6P7AZ','SELL',None,None,None,'MARKET',None,1)
        close_position(positions['dealId'].iloc[0], 'SELL', None, None,
                       None, 'MARKET', None, 1)

    #Hitting -500$ stop loss, actually can be skipped as create order has a stopLoss set
    # if (macd['Close'] < stop_loss):
    #     print('StopLoss hit because of -500$, closing BUY position')
    #     close_position(positions['dealId'].iloc[0], 'SELL', None, None,
    #                    None, 'MARKET', None, 1)
    # return target_profit


def execute(sc):
    create_session()
    results = ig_service.get_client_apps()
    timestr = time.strftime("%Y%m%d-%H%M%S")
    hour_file = 'hour-{}.csv'.format(timestr)
    macd_file = 'macd-{}.csv'.format(timestr)
    # get_epic_names('ASX').to_csv('epic_list.csv')
    # print(results)
    positions = get_open_positions()

    df_h = get_historical_data('IX.D.ASX.IFD.IP', 'H', 80)
    df_d = get_historical_data('IX.D.ASX.IFD.IP', 'D', 2)
    df_h.to_csv(hour_file)

    #df_h = pd.read_csv('hour_80.csv')
    # df_d = pd.read_csv('day.csv')
    pivot = calculate_pivot(df_d.iloc[0])
    # print(calculate_macd(df_h))
    macd_calculated = calculate_macd(df_h)
    macd_calculated.to_csv(macd_file)
    macd = macd_calculated.iloc[-1]
    macd_prev_int = macd_calculated.iloc[-2]

    # pivot = 7245.3
    # positions = pd.read_csv('position.csv') #test
    trigger_alert(macd, pivot, positions, macd_prev_int, 'IX.D.ASX.IFD.IP')

    s.enter(3600, 1, execute, (sc,))


if __name__ == "__main__":
    s.enter(1, 1, execute, (s,))
    s.run()
