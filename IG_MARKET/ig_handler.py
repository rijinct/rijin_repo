from ig_api_methods import close_position
from ig_api_methods import get_activity_by_period


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
    transaction = get_activity_by_period(21600000)
    if any(transaction.epic == instr):
        print('Already transacted in the last 3 hrs')
        return False
    else:
        print('No trasaction in the last 3 hrs')
        return True


def check_and_close_sell_position(diff, macd, positions, macd_prev_int, diff_prev_int):
    target_profit = positions['level'].iloc[0] - 3
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
    target_profit = positions['level'].iloc[0] + 3
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
