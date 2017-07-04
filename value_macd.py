# 可以自己import我们平台支持的第三方python模块，比如pandas、numpy等
import pandas as pd
import numpy as np
import datetime
import math
import talib
from collections import OrderedDict



# 在这个方法中编写任何的初始化逻辑, context对象将会在你的算法策略的任何方法之间做传递
def init(context):

    # 每周一按条件筛选公司并进行调仓
    scheduler.run_weekly(rebalance, 1)
    
    # 设置投资组合上限为20支
    context.totalstocks = 20
    
    # 投资组合中还有的空余位置
    context.availableslots = context.totalstocks

    # 设置每支股票的资金上限
    context.fundForEachStock = context.stock_account.cash / context.totalstocks
    logger.info("Fund for each stock is " + str(context.fundForEachStock))



# 每周一触发，该函数用于判断该股票是否连续若干周周收盘价上涨或下跌（周收盘价=周五收盘价）
def check_condition(stock, weeks, rise):

    logger.info(stock + " check " + str(weeks) + " rise?:" + str(rise))

    result = True
    historyprice = history_bars(stock, 150, '1d', ['datetime', 'close'])
    lastIndex = np.count_nonzero(historyprice) - 1

    # Dictionary - key is 日历中的第几周（相邻的数周一定不一样），value is 这周周五收盘价(如果周五停牌，就找周四、周三、周二、周一)在 historyprice 中的下标，每一周只有一天的数据
    indexesOfWeek = OrderedDict()
    
    # 往前回溯150个交易日，找出若干周的数据，今天的数据为 historyprice[149]，150个交易日前当天的数据为 historyprice[0]
    for day in range(0, lastIndex):
    
        i = lastIndex - day
        
        for j in range(0, 4):
            
            thisWeekDataIndex = i - j
            thisDayStr = str(historyprice[thisWeekDataIndex][0])
            thisDay = datetime.datetime.strptime(thisDayStr, '%Y%m%d%H%M%S')
            currentWeek = thisDay.isocalendar()[1]

            if currentWeek not in indexesOfWeek.keys():
                # 周一为0，周五为4
                if (thisDay.weekday() == 4):
                    if currentWeek not in indexesOfWeek.keys():
                        #logger.info(stock + ":Add Fri " + str(thisDay) + " " + str(thisWeekDataIndex) + " price: " + str(historyprice[thisWeekDataIndex][1]))
                        indexesOfWeek[currentWeek] = thisWeekDataIndex
                    break
                elif (thisDay.weekday() == 3):
                    if currentWeek not in indexesOfWeek.keys():
                        #logger.info(stock + ":Add Thurs " + str(thisDay) + " " + str(thisWeekDataIndex) + " price: " + str(historyprice[thisWeekDataIndex][1]))
                        indexesOfWeek[currentWeek] = thisWeekDataIndex
                    break
                elif (thisDay.weekday() == 2):
                    if currentWeek not in indexesOfWeek.keys():
                        #logger.info(stock + ":Add Wed " + str(thisDay) + " " + str(thisWeekDataIndex) + " price: " + str(historyprice[thisWeekDataIndex][1]))
                        indexesOfWeek[currentWeek] = thisWeekDataIndex
                    break
                elif (thisDay.weekday() == 1):
                    if currentWeek not in indexesOfWeek.keys():
                        #logger.info(stock + ":Add Tues " + str(thisDay) + " " + str(thisWeekDataIndex) + " price: " + str(historyprice[thisWeekDataIndex][1]))
                        indexesOfWeek[currentWeek] = thisWeekDataIndex
                    break
                elif (thisDay.weekday() == 0):
                    if currentWeek not in indexesOfWeek.keys():
                        #logger.info(stock + ":Add Mon " + str(thisDay) + " " + str(thisWeekDataIndex) + " price: " + str(historyprice[thisWeekDataIndex][1]))
                        indexesOfWeek[currentWeek] = thisWeekDataIndex
                    break
        
        if (len(indexesOfWeek) == weeks):
            break

    # 处理成正序周收盘价
    weeklyPriceReverseOrder = []
    
    for i in indexesOfWeek.keys():
        #logger.info(stock + " append " + str(historyprice[indexesOfWeek[i]][0]) + ":" + str(historyprice[indexesOfWeek[i]][1]))
        weeklyPriceReverseOrder.append(historyprice[indexesOfWeek[i]][1])
    
    weeklyPrice = weeklyPriceReverseOrder[::-1]

    # 周数据为正序，依次比较
    for nextWeekIndex in range(1, len(weeklyPrice)):
        
        thisWeekIndex = nextWeekIndex - 1

        thisWeekData = weeklyPrice[thisWeekIndex]
        nextWeekData = weeklyPrice[nextWeekIndex]
        #logger.info(stock + " compare " + str(thisWeekData) + " with " + str(nextWeekData))

        # 判断上涨
        if rise:
            # 上涨则本周股价低于下周
            result &= thisWeekData < nextWeekData
            
            if not result:
                #logger.info(stock + " " + str(weeks) + " rise:" + str(rise) + " fail because " + str(thisWeekData) + " >= " + str(nextWeekData))
                break
        
        else:
            # 下跌则反之
            result &= thisWeekData >= nextWeekData
            
            if not result:
                #logger.info(stock + " " + str(weeks) + " rise:" + str(rise) + " fail because " + str(thisWeekData) + " < " + str(nextWeekData))
                break

    return result



# 筛选股票函数
def filter_stocks(context):

    context.stocks = []

    #基于最新财务数据，筛选符合条件的股票
    context.fundamental_df = get_fundamentals(
        query(
            #ROE
            fundamentals.financial_indicator.return_on_equity,
            #流动比率 = 流动资产 / 流动负债
            fundamentals.financial_indicator.current_ratio,
            #毛利率
            fundamentals.financial_indicator.gross_profit_margin,
            #净利率
            fundamentals.financial_indicator.net_profit_margin,
            #营业收入增长率
            fundamentals.financial_indicator.inc_operating_revenue,
            #营业利润增长率
            fundamentals.financial_indicator.inc_gross_profit,
            #资产负债率
            fundamentals.financial_indicator.debt_to_asset_ratio,
            #经营性现金流
            fundamentals.cash_flow_statement.cash_flow_from_operating_activities,
            #营业收入
            fundamentals.income_statement.revenue,
            #净利润
            fundamentals.income_statement.net_profit,
            #总资产
            fundamentals.balance_sheet.total_assets,
            #市盈率
            fundamentals.eod_derivative_indicator.pe_ratio,
            #市净率
            fundamentals.eod_derivative_indicator.pb_ratio
        )
        #.filter(
        #    #ROE >= 15
        #    fundamentals.financial_indicator.return_on_equity >= 15
        #)
        #.filter(
        #    #流动资产 >= 1.5倍流动负债
        #    fundamentals.financial_indicator.current_ratio >= 1.5
        #)
        .filter(
            #毛利率大于50%
            fundamentals.financial_indicator.gross_profit_margin >= 0.5
        )
        .filter(
            #净利率大于20%
            fundamentals.financial_indicator.inc_operating_revenue >= 0.2
        )
        .filter(
            #营业收入增长率大于20%
            fundamentals.financial_indicator.net_profit_margin >= 0.2
        )
        .filter(
            #营业利润增长率大于20%
            fundamentals.financial_indicator.inc_gross_profit >= 0.2
        )
        #.filter(
        #    #资产负债率 <= 50%
        #    fundamentals.financial_indicator.debt_to_asset_ratio <= 0.5
        #)
        .filter(
            #经营性现金流为正
            fundamentals.cash_flow_statement.cash_flow_from_operating_activities > 0
        )
        #.filter(
        #    #营业收入10亿以上
        #    fundamentals.income_statement.revenue >= 1000000000
        #)
        #.filter(
        #    #净利润1亿以上
        #    fundamentals.income_statement.net_profit >= 100000000
        #)
        #.filter(
        #    #PE * PB <= 40
        #    fundamentals.eod_derivative_indicator.pe_ratio * fundamentals.eod_derivative_indicator.pb_ratio <= 40
        #)
        .order_by(
            fundamentals.financial_indicator.inc_operating_revenue
        )
        .limit(
            20
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
            logger.info(str(stock) + " rises for 13 weeks")
            sell_stock(context, stock, 1)

        # 如果该股票连续上涨9周，卖目前其份额的1/2
        elif check_condition(stock, 9, True):
            logger.info(str(stock) + " rises for 9 weeks")
            sell_stock(context, stock, 0.5)

        # 如果该股票连续上涨7周，卖目前其份额的1/3
        elif check_condition(stock, 7, True):
            logger.info(str(stock) + " rises for 7 weeks")
            sell_stock(context, stock, 0.33)

        # 如果该股票连续下跌7周，如果在投资组合中，购买其分配资金池的1/3
        elif check_condition(stock, 7, False) and (not check_condition(stock, 8, False)):
            logger.info(str(stock) + " falls for 7 weeks")
            stocks_tobuy[str(stock)] = 0.33

        # 如果该股票连续下跌9周，如果在投资组合中，购买其分配资金池的1/2
        elif check_condition(stock, 9, False) and (not check_condition(stock, 10, False)):
            logger.info(str(stock) + " falls for 9 weeks")
            stocks_tobuy[str(stock)] = 0.5

        # 如果该股票连续下跌13周，如果在投资组合中，购买其分配资金池的全部
        elif check_condition(stock, 13, False) and (not check_condition(stock, 14, False)):
            logger.info(str(stock) + " falls for 13 weeks")
            stocks_tobuy[str(stock)] = 1

    # 买入操作
    logger.info("Stocks to watch: " + str(len(stocks_tobuy)))
    buy_stock(context, stocks_tobuy)



# 调整投资组合权重平仓
def sell_stock(context, stock, percentage):

    context.availableslots = context.totalstocks - len(context.portfolio.positions)

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
        
        # 如果交易操作为清空该股票，腾出投资组合中一支股票的空间
        if percentage == 1:
            context.availableslots += 1
            logger.info("Sold out " + stock + ", available slots are " + str(context.availableslots))



# 调整投资组合权重以调仓
def buy_stock(context, stocks_tobuy):

    context.availableslots = context.totalstocks - len(context.portfolio.positions)

    if len(stocks_tobuy) > 0:

        # 连跌7周的股票组合
        stocks_onethirdbuy = []
    
        # 连跌9周的股票组合
        stocks_halfbuy = []
    
        # 连跌13周的股票组合
        stocks_allbuy = []
    
        # 按照筛选出来的买入股票列表的优先级，调整仓位
        for stock in stocks_tobuy:
    
            if stocks_tobuy[stock] == 0.33:
                stocks_onethirdbuy.append(stock)
    
            elif stocks_tobuy[stock] == 0.5:
                stocks_halfbuy.append(stock)
    
            elif stocks_tobuy[stock] == 1:
                stocks_allbuy.append(stock)

        if context.availableslots > 0:
            # 如果投资组合中还有空位，优先买入连跌7周的股票
            for stock in stocks_onethirdbuy:
                
                onethirdbuy_cash_eachstock = context.fundForEachStock / 3
                
                if stock in context.portfolio.positions:
                    # 如果该股票目前在投资组合中，调仓至1/3
                    
                    logger.info(stock + " is already in portfolio, adjust to 1/3 = " + str(onethirdbuy_cash_eachstock))
                    order_target_value(stock, onethirdbuy_cash_eachstock)
                    
                else:
                    # 如果该股票目前不在投资组合中，加入并买1/3
                    logger.info("Buy 1/3 = " + str(onethirdbuy_cash_eachstock) + " of " + stock)
                    order_value(stock, onethirdbuy_cash_eachstock)
                    context.availableslots -= 1
                    logger.info("Add " + stock + " to portfolio, available slots are " + str(context.availableslots))
                    
                    if context.availableslots == 0:
                        break

        if context.availableslots > 0:
            # 如果还有剩余空位，购买连跌9周的股票
            for stock in stocks_halfbuy:
    
                halfbuy_cash_eachstock = context.fundForEachStock / 2
                
                if stock in context.portfolio.positions:
                    # 如果该股票目前在投资组合中，调仓至1/2
                    
                    logger.info(stock + " is already in portfolio, adjust to 1/2 = " + str(halfbuy_cash_eachstock))
                    order_target_value(stock, halfbuy_cash_eachstock)
                    
                else:
                    # 如果该股票目前不在投资组合中，加入并买1/2
                    logger.info("Buy 1/2 = " + str(halfbuy_cash_eachstock) + " of " + stock)
                    order_value(stock, halfbuy_cash_eachstock)
                    context.availableslots -= 1
                    logger.info("Add " + stock + " to portfolio, available slots are " + str(context.availableslots))
                    
                    if context.availableslots == 0:
                        break

        if context.availableslots > 0:
            # 如果还有剩余空位，购买连跌13周的股票
            for stock in stocks_allbuy:
    
                allbuy_cash_eachstock = context.fundForEachStock
                
                if stock in context.portfolio.positions:
                    # 如果该股票目前在投资组合中，满仓
                    
                    logger.info(stock + " is already in portfolio, adjust to 100% = " + str(allbuy_cash_eachstock))
                    order_target_value(stock, allbuy_cash_eachstock)
                    
                else:
                    # 如果该股票目前不在投资组合中，加入并满仓
                    logger.info("Buy 100% = " + str(allbuy_cash_eachstock) + " of " + stock)
                    order_value(stock, allbuy_cash_eachstock)
                    context.availableslots -= 1
                    logger.info("Add " + stock + " to portfolio, available slots are " + str(context.availableslots))
                    
                    if context.availableslots == 0:
                        break


