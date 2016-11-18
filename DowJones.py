"""
This is a template algorithm on Quantopian for you to adapt and fill in.
"""
from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import AverageDollarVolume, CustomFactor
from quantopian.pipeline.filters.morningstar import Q500US
from quantopian.pipeline.data import morningstar

import numpy as np
from datetime import datetime

stockList=[]
class SidFactor(CustomFactor):
    inputs = []
    window_length = 1
    sids = []

    def compute(self, today, assets, out):
        out[:] = np.in1d(assets, self.sids)

# Calculate custom market capitalization based on closing price and outstanding shares.
class MarketCap(CustomFactor):

    # Pre-declare inputs and window_length
    inputs = [USEquityPricing.close, morningstar.valuation.shares_outstanding]
    window_length = 1

  # Compute market cap value
    def compute(self, today, assets, out, close, shares):
        out[:] = close[-1] * shares[-1]

def initialize(context):
    """
    Called once at the start of the algorithm.
    """
#    context.max_notional = 100000
    context.stocks = [sid(4922), sid(679), sid(24), sid(698),sid(1267),sid(23112),sid(1900),sid(4283),sid(2119),sid(2119),sid(8347),sid(3149),sid(20088),sid(3496),sid(3951),sid(3766),sid(4151),sid(25006),sid(4707),sid(5029),sid(5061),sid(5328),sid(5923),sid(5938),sid(24845),sid(7792),sid(7883),sid(21839),sid(35920),sid(8229),sid(2190)]

    set_benchmark(symbol('SPY'))

    schedule_function(schedule_tasks, date_rules.month_start(days_offset = 0))

    # Record tracking variables at the end of each day.
    schedule_function(my_record_vars, date_rules.every_day(), time_rules.market_close())

    # Create our dynamic stock selector.

    attach_pipeline(make_pipeline(context), 'my_pipeline')


def make_pipeline(context):

    sid     = SidFactor()
    sid.sids= context.stocks
    mkt_cap = MarketCap()

    pipe = Pipeline(
        columns={
                'mkt_cap' : mkt_cap,
            },
        screen = sid.eq(1)
        )

    return pipe;


def schedule_tasks(context, data):
    today = get_datetime('US/Eastern')

    if(today.month == 12):
        do_annually(context, data)

def do_annually(context, data):
    log.info(" Annual rebalance task")
    my_rebalance(context, data)


def before_trading_start(context, data):
    """
    Called every day before market open.
    """
    context.output = pipeline_output('my_pipeline')


def my_assign_weights(context, data):
    """
    Assign weights to securities that we want to order.
    """
    context.totalMktCap = context.output['mkt_cap'].sum()
    for stock in context.stocks:
        log.info("Market cap %s %d ", (str(stock), context.output[stock].mkt_cap))

    context.output['weights'] = context.output / context.totalMktCap

def my_rebalance(context,data):
    """
    Execute orders according to our schedule_function() timing.
    """
    my_assign_weights(context, data)

    if has_orders(context):
        log.info('pendign orders. Returning ! ')
        return

    for sec in context.output.index:
        order_target_percent(sec, context.output.ix[sec].weights, limit_price=None, stop_price = None)


def has_orders(context):
    # Return true if there are pending orders
    has_orders = False
    for stock in context.stocks:
        orders = get_open_orders(stock)
        if orders:
            for oo in orders:
                message = 'Open order for {amount} shares in {stock}'
                message = message.format(amount = oo.amount, stock = sec)
                log.info(message)

            has_orders = true
    return has_orders

def my_record_vars(context, data):
    """
    Plot variables at the end of each day.
    """
    record(num_pos = len(context.portfolio.positions))
