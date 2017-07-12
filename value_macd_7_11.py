# 可以自己import我们平台支持的第三方python模块，比如pandas、numpy等
import pandas as pd
import numpy as np
import datetime
import math
import talib
from collections import OrderedDict



# 在这个方法中编写任何的初始化逻辑, context对象将会在你的算法策略的任何方法之间做传递
def init(context):

    # 每月第22个交易日筛选公司
    scheduler.run_monthly(filter_stocks, 22)

    # 每周一根据最新一季财报筛选的公司，观望交易机会并进行调仓
    scheduler.run_weekly(rebalance, 1)
    
    # 最新一季财报符合筛选条件的股票列表
    context.stocks = []
    
    # 设置投资组合股票数量上限，等权重分配资金
    context.totalstocks = 30
    
    # 投资组合中还有的空余位置
    context.availableslots = context.totalstocks

    # 设置每支股票的资金上限
    context.fundForEachStock = context.stock_account.cash / context.totalstocks
    logger.info("Fund for each stock is " + str(context.fundForEachStock))
    
    # 参数
    
    # 买入条件的周换手率为 <= X%
    context.turnoverrate_buy = 4
    
    # 卖出条件的周换手率为 >= X%
    context.turnoverrate_sell = 7
    
    # 强制清仓时的收益率
    context.selloutgrowth = 0.2
    
    # 买卖1/3的条件 - 连续涨跌的星期数
    context.onethirdtransaction = 5
    
    # 买卖2/3的条件 - 连续涨跌的星期数
    context.halftransaction = 7
    
    # 全买卖的条件 - 连续涨跌的星期数
    context.fulltransaction = 9



# 每周一触发，该函数用于判断该股票是否连续若干周周收盘价上涨或下跌（周收盘价=周五收盘价）
def check_condition(stock, weeks, rise):

    #logger.info(stock + " check " + str(weeks) + " rise?:" + str(rise))

    result = True
    historyprice = history_bars(stock, 150, '1d', ['datetime', 'close'])
    lastIndex = np.count_nonzero(historyprice) - 1

    # Dictionary - key is 日历中的第几周（相邻的数周该数值不同），value is 这周周五收盘价(如果周五停牌，就找周四、周三、周二、周一)在 historyprice 中的下标，每一周只有一天的数据
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
def filter_stocks(context, bar_dict):

    #基于最新一季的财务数据，筛选符合条件的股票
    context.fundamental_df = get_fundamentals(
        query(
            #ROE
            fundamentals.financial_indicator.return_on_equity,
            #营业收入同比增长率
            fundamentals.financial_indicator.inc_operating_revenue,
            #扣除非经常损益净利润
            fundamentals.financial_indicator.adjusted_net_profit,
            #扣除非经常损益净利润同比增长率
            fundamentals.financial_indicator.inc_adjusted_net_profit,
            #经营性现金流同比增长率
            fundamentals.financial_indicator.inc_cash_from_operations,
            #经营性现金流净额
            fundamentals.cash_flow_statement.cash_flow_from_operating_activities
        )
        .filter(
            #ROE >= 15
            fundamentals.financial_indicator.return_on_equity >= 15
        )
        .filter(
            #营业收入同比增长30%
            fundamentals.financial_indicator.inc_operating_revenue >= 0.3
        )
        .filter(
            #扣非净利润率同比增长30%
            fundamentals.financial_indicator.inc_adjusted_net_profit >= 0.3
        )
        .filter(
            #经营性现金流同比增长
            fundamentals.financial_indicator.inc_operating_revenue > 0
        )
        .filter(
            #经营性现金流为正
            fundamentals.financial_indicator.inc_adjusted_net_profit > 0
        )
        .order_by(
            fundamentals.financial_indicator.inc_adjusted_net_profit.desc()
        ),
        entry_date = None,
        interval = '1y',
        report_quarter = 'Q1'
    )
    
    context.stocks = []
    
    for stock in context.fundamental_df.columns.values:
        if instruments(stock).days_from_listed() > 180:
            context.stocks.append(stock)
    
    logger.info("Filtered " + str(len(context.stocks)) + " stocks")



# 调仓函数
def rebalance(context, bar_dict):

    # 买入股票列表
    stocks_tobuy = {}

    # 如果投资组合中的股票已经不在新一季的符合筛选条件的公司中，清空
    for holdstock in context.portfolio.positions:
    
        if holdstock not in context.stocks:
            logger.info(str(holdstock) + " is not in filtered stock list, selling all shares")
            sell_stock(context, holdstock, 1, True)

    # 对每支股票进行买入或卖出条件检查
    for stock in context.stocks:

        # 如果该股票满足平仓条件，清空
        if check_condition(stock, context.fulltransaction, True):
            logger.info(str(stock) + " keeps rising for " + str(context.fulltransaction) + " weeks")
            sell_stock(context, stock, 1)

        # 如果该股票满足卖出目前其份额的2/3的条件
        elif check_condition(stock, context.halftransaction, True):
            logger.info(str(stock) + " keeps rising for " + str(context.halftransaction) + " weeks")
            sell_stock(context, stock, 0.66)

        # 如果该股票满足买入2/3的条件，如果在筛选结果中，购买其分配资金池的2/3
        elif check_condition(stock, context.halftransaction, False) and (not check_condition(stock, context.halftransaction + 1, False)):
            logger.info(str(stock) + " keeps falling for " + str(context.halftransaction) + " weeks")
            stocks_tobuy[str(stock)] = 0.66

        # 如果该股票满足满仓的条件，如果在筛选结果中，购买其分配资金池的全部
        elif check_condition(stock, context.fulltransaction, False) and (not check_condition(stock, context.fulltransaction + 1, False)):
            logger.info(str(stock) + " keeps falling for " + str(context.fulltransaction) + " weeks")
            stocks_tobuy[str(stock)] = 1

    # 买入操作
    logger.info("Stocks to watch: " + str(len(stocks_tobuy)))
    buy_stock(context, stocks_tobuy)



# 调整投资组合权重平仓
def sell_stock(context, stock, percentage, force = False):

    context.availableslots = context.totalstocks - len(context.portfolio.positions)

    # 如果该股票在投资组合中
    if stock in context.portfolio.positions:
    
        # force为True时，因为该股票以及不在最新一季的筛选股票名单中，所以忽略周转率强制清空
        if force:
            # 如果满足收益条件，盈利 >= 建仓成本 * 收益比率
            if context.portfolio.positions[stock].pnl >= context.portfolio.positions[stock].avg_price * context.portfolio.positions[stock].quantity * context.selloutgrowth:
                order_target_percent(stock, 0)
                context.availableslots = context.totalstocks - len(context.portfolio.positions)
        
        # force为False时，为根据涨跌买卖的情况，只交易当周换手率为特定条件的股票
        else:
        
            weekTurnoverRates = get_turnover_rate(stock).week
            currWeekRate = weekTurnoverRates[len(weekTurnoverRates) - 1]
            logger.info(stock + " weekly turnover rate is " + str(currWeekRate))
    
            if (currWeekRate >= context.turnoverrate_sell):
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
                    context.availableslots = context.totalstocks - len(context.portfolio.positions)
                    logger.info("Sold out " + stock + ", available slots are " + str(context.availableslots))



# 调整投资组合权重以调仓
def buy_stock(context, stocks_tobuy):

    context.availableslots = context.totalstocks - len(context.portfolio.positions)

    if len(stocks_tobuy) > 0:

        # 购买2/3的股票组合
        stocks_halfbuy = []
    
        # 满仓的股票组合
        stocks_allbuy = []
    
        # 按照筛选出来的买入股票列表的优先级，调整仓位
        for stock in stocks_tobuy:
    
            # 只交易当周换手率为特定条件的股票
            weekTurnoverRates = get_turnover_rate(stock).week
            currWeekRate = weekTurnoverRates[len(weekTurnoverRates) - 1]
            logger.info(stock + " weekly turnover rate is " + str(currWeekRate))
            
            if currWeekRate <= context.turnoverrate_buy:
                if stocks_tobuy[stock] == 0.33:
                    stocks_onethirdbuy.append(stock)
        
                elif stocks_tobuy[stock] == 0.66:
                    stocks_halfbuy.append(stock)
        
                elif stocks_tobuy[stock] == 1:
                    stocks_allbuy.append(stock)

        if context.availableslots > 0:
            # 如果还有剩余空位，购买2/3的股票
            for stock in stocks_halfbuy:
    
                halfbuy_cash_eachstock = context.fundForEachStock * 2 / 3
                
                if stock not in context.portfolio.positions:
                    # 如果该股票目前不在投资组合中，加入并买2/3
                    logger.info("Buy 2/3 = " + str(halfbuy_cash_eachstock) + " of " + stock)
                    order_value(stock, halfbuy_cash_eachstock)
                    context.availableslots = context.totalstocks - len(context.portfolio.positions)
                    logger.info("Add " + stock + " to portfolio, available slots are " + str(context.availableslots))
                    
                    if context.availableslots == 0:
                        break

        if context.availableslots > 0:
            # 如果还有剩余空位，购入满仓的股票
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
                    context.availableslots = context.totalstocks - len(context.portfolio.positions)
                    logger.info("Add " + stock + " to portfolio, available slots are " + str(context.availableslots))
                    
                    if context.availableslots == 0:
                        break


