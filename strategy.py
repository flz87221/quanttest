# 可以自己import我们平台支持的第三方python模块，比如pandas、numpy等
import pandas as pd
import numpy as np
import datetime
import math
import talib



# 在这个方法中编写任何的初始化逻辑, context对象将会在你的算法策略的任何方法之间做传递
def init(context):

    #注册指标函数，计算股价调整
    reg_indicator('adjust', priceadjust, '1d', win_size = 5)

    #暂定每月第十六个交易日，按条件筛选公司并补仓
    scheduler.run_monthly(rebalance, 16)



# TODO - 股价调整判断函数
def priceadjust():

    return CROSS(MA(CLOSE, 5), MA(CLOSE, 30))



# 7周每周均线连续上涨判断函数
def checksellcondition(stock, bar_dict):

    sell = True
    
    logger.info("Verify weekly MA for " + stock)
    twomonthprice = history_bars(stock, 60, '1d', 'close')
    weekMA = talib.MA(twomonthprice, 5)
    
    for i in range(1, 7):

        # weekMA[0 - i] 为本周均线，weekMA[0 - i * 5 - 1] 为上周均线
        if weekMA[0 - i] > weekMA[0 - i * 5 - 1]:
            sell &= True

    return sell



# 盯盘函数，你选择的证券的数据更新将会触发此段逻辑，如果是每日回测，该函数触发频率为每日
def handle_bar(context, bar_dict):
    # bar_dict[order_book_id] 可以拿到某个证券的bar信息
    # context.portfolio 可以拿到现在的投资组合信息

    #如果投资组合中的某支股票已经连续上涨7周，平仓
    for stock in context.portfolio.positions:

        #如果该股票连续上涨7周
        if checksellcondition(stock, bar_dict):
        
            #平仓
            logger.info("Sold " + stock)
            order_target_percent(stock, 0)



# 筛选股票函数
def filter_stocks(context):

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
        .filter(
            #ROE >= 15
            fundamentals.financial_indicator.return_on_equity >= 15
        )
        .filter(
            #流动资产 >= 1.5倍流动负债
            fundamentals.financial_indicator.current_ratio >= 1.5
        )
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
        .filter(
            #营业收入10亿以上
            fundamentals.income_statement.revenue >= 1000000000
        )
        .filter(
            #净利润1亿以上
            fundamentals.income_statement.net_profit >= 100000000
        )
        #.filter(
        #    #PE * PB <= 40
        #    fundamentals.eod_derivative_indicator.pe_ratio * fundamentals.eod_derivative_indicator.pb_ratio <= 40
        #)
        .order_by(
            fundamentals.eod_derivative_indicator.market_cap.desc()
        )
    )

    context.stocks = context.fundamental_df.columns.values



# 调仓,context.fundamental_df中是最近一次筛选出的"好公司"股票
def rebalance(context, bar_dict):

    #筛选公司
    filter_stocks(context)

    weight = update_weights(context, context.stocks)

    for stock in context.fundamental_df:

        if get_indicator(stock, 'adjust'):

            if weight != 0 and stock in context.fundamental_df:

                logger.info("Buy " + stock + " with weight " + str(weight))
                order_target_percent(stock, weight)



# 根据"好公司"列表的公司数量，等权重分配
def update_weights(context, stocks):

    if len(stocks) == 0:
        return 0

    else:
        weight = .95/len(stocks)
        return weight