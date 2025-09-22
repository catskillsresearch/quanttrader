import unittest
from unittest.mock import MagicMock, patch
from quanttrader.brokerage.schwab_brokerage import SchwabBrokerage
from quanttrader.event.live_event_engine import LiveEventEngine
from quanttrader.order.order_event import OrderEvent, OrderType
from quanttrader.order.order_status import OrderStatus

class TestSchwabBrokerage(unittest.TestCase):
    def setUp(self):
        self.msg_engine = LiveEventEngine()
        self.tick_engine = LiveEventEngine()
        self.api_key = "dummy_key"
        self.app_secret = "dummy_secret"
        self.callback_url = "http://localhost"
        self.token_path = "token.json"

        # Patch Schwab auth and client
        patcher = patch("quanttrader.brokerage.schwab_brokerage.auth.easy_client")
        self.mock_client = patcher.start()
        self.addCleanup(patcher.stop)

        self.mock_client.return_value = MagicMock()
        self.broker = SchwabBrokerage(self.msg_engine, self.tick_engine,
                                      self.api_key, self.app_secret,
                                      self.callback_url, self.token_path)

    def test_connect_disconnect(self):
        self.broker.connect()
        self.broker.disconnect()

    def test_place_order(self):
        order = OrderEvent()
        order.full_symbol = "AAPL"
        order.order_size = 10
        order.order_type = OrderType.LIMIT
        order.limit_price = 150.00

        self.broker.place_order(order)
        self.assertEqual(order.order_status, OrderStatus.ACKNOWLEDGED)
        self.assertIn(order.order_id, self.broker.order_dict)

    def test_cancel_order(self):
        order = OrderEvent()
        order.full_symbol = "AAPL"
        order.order_size = 10
        order.order_type = OrderType.LIMIT
        order.limit_price = 150.00

        self.broker.place_order(order)
        self.broker.cancel_order(order.order_id)
        self.assertEqual(self.broker.order_dict[order.order_id].order_status, OrderStatus.CANCELED)

    def test_subscribe_account_summary(self):
        self.broker.client.get_account_info.return_value = {
            "cashBalance": "10000.00",
            "availableFunds": "8000.00"
        }
        self.broker.subscribe_account_summary()
        self.assertEqual(self.broker.account_summary.balance, 10000.00)

    def test_subscribe_positions(self):
        self.broker.client.get_positions.return_value = [
            {"symbol": "AAPL", "quantity": 10, "averagePrice": 145.00}
        ]
        self.broker.subscribe_positions()

    def test_request_historical_data(self):
        self.broker.client.get_price_history_every_day.return_value.json.return_value = {
            "candles": [
                {"datetime": 1695000000000, "close": 150.00},
                {"datetime": 1695086400000, "close": 152.00}
            ]
        }
        self.broker.request_historical_data("AAPL")

if __name__ == "__main__":
    unittest.main()
