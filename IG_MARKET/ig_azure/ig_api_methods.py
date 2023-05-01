from trading_ig import IGService

from config import config


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
    response = ig_service.fetch_historical_prices_by_epic_and_num_points(epic,resolution,num_points)
    print("epic:{e},resolution:{r},num_point:{n}".format(e=epic,r=resolution,n=num_points))
    print(response)
    df_ask = response['prices']['ask']
    df_last = response['prices']['last']
    df_ask['Volume'] = df_last['Volume']
    #exit(0)

    return df_ask

def get_activity_by_period(time):
    return ig_service.fetch_account_activity_by_period(time)

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
