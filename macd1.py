# 可以自己import我们平台支持的第三方python模块，比如pandas、numpy等
import pandas as pd
import numpy as np
import datetime
import math
import talib



# 在这个方法中编写任何的初始化逻辑, context对象将会在你的算法策略的任何方法之间做传递
def init(context):

    # 每周一按条件筛选公司并进行调仓
    scheduler.run_weekly(rebalance, 1)



# 每周一触发，判断连续若干周上涨或下跌（周价位=周五收盘价）
def check_condition(stock, weeks, rise):

    result = True
    lastIndex = 5 * weeks;
    historyprice = history_bars(stock, 120, '1d', 'close')

    # 逐周比较
    for i in range(0, weeks - 1):

        # 本周一减一个工作日为上周五，减六个工作日为上上周五
        latestFridayIndex = lastIndex - 1 - i * 5
        lastFridayIndex = lastIndex - 1 - (i + 1) * 5

        if latestFridayIndex >= 0 and lastFridayIndex >= 0 and latestFridayIndex < len(historyprice) and lastFridayIndex < len(historyprice):

            latestFriday = historyprice[latestFridayIndex]
            lastFriday = historyprice[lastFridayIndex]

            # 判断上涨
            if rise:
                # 上涨则本周股价高于上周
                result &= latestFriday > lastFriday
            else:
                # 下跌则反之
                result &= latestFriday < lastFriday

        else:

            result = False

    return result



# 筛选股票函数
def filter_stocks(context):

    context.stocks = []

    # 返回所有股票
    context.fundamental_df = get_fundamentals(
        query(
            fundamentals.eod_derivative_indicator.market_cap
        )
        .order_by(
            fundamentals.eod_derivative_indicator.market_cap.desc()
        )
    )

    for stock in context.fundamental_df.columns.values:
        if instruments(stock).days_from_listed() > 180:
            context.stocks.append(stock)
    
    logger.info("Filtered " + str(len(context.stocks)) + " stocks")



# 调仓函数
def rebalance(context, bar_dict):

    # 获取全部股票
    filter_stocks(context)
    
    # 买入股票列表
    stocks_tobuy = {}

    # 对每支股票进行买入或卖出条件检查
    for stock in context.stocks:

        # 如果该股票连续上涨13周，卖光
        if check_condition(stock, 13, True):
            sell_stock(context, stock, 1)

        # 如果该股票连续上涨9周，卖目前其份额的1/2
        elif check_condition(stock, 9, True):
            sell_stock(context, stock, 0.5)

        # 如果该股票连续上涨7周，卖目前其份额的1/3
        elif check_condition(stock, 7, True):
            sell_stock(context, stock, 0.33)

        # 如果该股票连续下跌13周，剩余资金全买
        elif check_condition(stock, 13, False):
            stocks_tobuy[str(stock)] = 1

        # 如果该股票连续下跌9周，用剩余资金的1/2购买
        elif check_condition(stock, 9, False):
            stocks_tobuy[str(stock)] = 0.5

        # 如果该股票连续下跌7周，用剩余资金的1/3购买
        elif check_condition(stock, 7, False):
            stocks_tobuy[str(stock)] = 0.33

    # 买入操作
    logger.info("Length of stocks_tobuy: " + str(len(stocks_tobuy)))
    buy_stock(context, stocks_tobuy)



# 调整投资组合权重平仓
def sell_stock(context, stock, percentage):

    # 如果该股票在投资组合中
    if stock in context.portfolio.positions:

        logger.info("Sell stock " + stock + " with percentage " + str(percentage * 100) + "%")

        # 获取当前该股票在投资组合中的权重
        curweight = context.portfolio.positions[stock].value_percent
        logger.info("Stock " + stock + "'s current weight is " + str(curweight * 100) + "%")

        # 设置新权重，percentage为卖掉目前其份额的多少
        newweight = curweight * (1 - percentage)

        # 根据新权重，卖出股票份额
        logger.info("Update " + stock + "'s new weight to " + str(newweight * 100) + "%")
        order_target_percent(stock, newweight)



# 调整投资组合权重建仓
def buy_stock(context, stocks_tobuy):

    if len(stocks_tobuy) > 0:

        # 连跌13周的股票组合
        stocks_allbuy = []
    
        # 连跌9周的股票组合
        stocks_halfbuy = []
    
        # 连跌7周的股票组合
        stocks_onethirdbuy = []
    
        # 按照筛选出来的买入股票列表的优先级，调整仓位
        for stock in stocks_tobuy:
    
            if stocks_tobuy[stock] == 1:
                stocks_allbuy.append(stock)
    
            elif stocks_tobuy[stock] == 0.5:
                stocks_halfbuy.append(stock)
    
            elif stocks_tobuy[stock] == 0.33:
                stocks_onethirdbuy.append(stock)

        # 如果有剩余资金，优先购买连跌13周的股票组合，股票之间为均等权重
        if len(stocks_allbuy) > 0 and context.portfolio.cash > 0:
    
            allbuy_cash_eachstock = context.portfolio.cash / len(stocks_allbuy)

            for stock in stocks_allbuy:
                logger.info("Buy " + str(allbuy_cash_eachstock) + " " + stock)
                order_value(stock, allbuy_cash_eachstock)

        # 如果还有剩余资金，购买连跌9周的股票组合，股票之间为均等权重
        if len(stocks_halfbuy) > 0 and context.portfolio.cash > 0:

            halfbuy_cash_eachstock = context.portfolio.cash / len(stocks_halfbuy)

            for stock in stocks_halfbuy:
                logger.info("Buy " + str(halfbuy_cash_eachstock) + " " + stock)
                order_value(stock, halfbuy_cash_eachstock)

        # 如果还有剩余资金，购买连跌7周的股票组合，股票之间为均等权重
        if len(stocks_onethirdbuy) > 0 and context.portfolio.cash > 0:

            onethirdbuy_cash_eachstock = context.portfolio.cash / len(stocks_onethirdbuy)

            for stock in stocks_onethirdbuy:
                logger.info("Buy " + str(onethirdbuy_cash_eachstock) + " " + stock)
                order_value(stock, onethirdbuy_cash_eachstock)


