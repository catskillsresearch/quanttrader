"""NOTES

This implementation assumes Schwab-Py’s client.place_order() and client.get_positions() return JSON-like dicts.

You’ll need to adapt symbol formatting and order translation to Schwab’s API schema.

Market data streaming and depth are not supported by Schwab-Py yet, so those methods are omitted. ??? I've done it before?

"""

import logging
import pandas as pd
from datetime import datetime
from copy import copy
from schwab import auth, client

from ..event.live_event_engine import LiveEventEngine
from ..order.order_event import OrderEvent
from ..order.order_status import OrderStatus
from ..order.order_type import OrderType
from ..position.position_event import PositionEvent
from ..account.account_event import AccountEvent
from ..data.tick_event import TickEvent, TickType as QtTickType
from .brokerage_base import BrokerageBase

_logger = logging.getLogger(__name__)
__all__ = ["SchwabBrokerage"]

class SchwabBrokerage(BrokerageBase):
    def __init__(self, msg_event_engine: LiveEventEngine, tick_event_engine: LiveEventEngine,
                 api_key: str, app_secret: str, callback_url: str, token_path: str):
        super().__init__()
        self.event_engine = msg_event_engine
        self.tick_event_engine = tick_event_engine
        self.client = auth.easy_client(api_key, app_secret, callback_url, token_path)
        self.account_summary = AccountEvent()
        self.account_summary.brokerage = "Schwab"
        self.order_dict = {}
        self.orderid = 1

    def connect(self):
        _logger.info("Connected to Schwab API")

    def disconnect(self):
        _logger.info("Disconnected from Schwab API")

    def place_order(self, order_event: OrderEvent):
        if not self.client:
            return

        order_event.order_id = self.orderid
        self.orderid += 1
        order_event.timestamp = pd.Timestamp.now()
        order_event.order_status = OrderStatus.ACKNOWLEDGED
        self.order_dict[order_event.order_id] = order_event
        self.event_engine.put(copy(order_event))

        # Schwab API call
        response = self.client.place_order(order_event.full_symbol, {
            "quantity": abs(order_event.order_size),
            "orderType": self._map_order_type(order_event),
            "price": order_event.limit_price,
            "side": "BUY" if order_event.order_size > 0 else "SELL"
        })
        _logger.info(f"Order placed: {response}")

    def cancel_order(self, order_id: int):
        if order_id not in self.order_dict:
            _logger.error(f"Order not found: {order_id}")
            return
        self.client.cancel_order(order_id)
        self.order_dict[order_id].order_status = OrderStatus.CANCELED
        self.event_engine.put(copy(self.order_dict[order_id]))

    def subscribe_account_summary(self):
        account_info = self.client.get_account_info()
        self.account_summary.balance = float(account_info["cashBalance"])
        self.account_summary.available = float(account_info["availableFunds"])
        self.account_summary.timestamp = datetime.now().strftime("%H:%M:%S.%f")
        self.event_engine.put(self.account_summary)

    def subscribe_positions(self):
        positions = self.client.get_positions()
        for pos in positions:
            position_event = PositionEvent()
            position_event.full_symbol = pos["symbol"]
            position_event.size = int(pos["quantity"])
            position_event.average_cost = float(pos["averagePrice"])
            position_event.timestamp = datetime.now().strftime("%H:%M:%S.%f")
            self.event_engine.put(position_event)

    def request_historical_data(self, symbol: str):
        response = self.client.get_price_history_every_day(symbol)
        for bar in response.json()["candles"]:
            tick = TickEvent()
            tick.full_symbol = symbol
            tick.timestamp = pd.to_datetime(bar["datetime"], unit="ms")
            tick.price = bar["close"]
            tick.tick_type = QtTickType.TRADE
            self.tick_event_engine.put(tick)

    def _map_order_type(self, order_event: OrderEvent):
        if order_event.order_type == OrderType.MARKET:
            return "MARKET"
        elif order_event.order_type == OrderType.LIMIT:
            return "LIMIT"
        elif order_event.order_type == OrderType.STOP:
            return "STOP"
        elif order_event.order_type == OrderType.STOP_LIMIT:
            return "STOP_LIMIT"
        return "UNKNOWN"
