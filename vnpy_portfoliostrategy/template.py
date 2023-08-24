from abc import ABC
from copy import copy
from typing import Dict, Set, List, TYPE_CHECKING, Optional
from collections import defaultdict

from vnpy.trader.constant import Interval, Direction, Offset
from vnpy.trader.object import BarData, TickData, OrderData, TradeData
from vnpy.trader.utility import virtual

from .base import EngineType

if TYPE_CHECKING:
    from .engine import StrategyEngine


class StrategyTemplate(ABC):
    """Portfolio Strategy Template"""

    author: str = ""
    parameters: list = []
    variables: list = []

    def __init__(
        self,
        strategy_engine: "StrategyEngine",
        strategy_name: str,
        vt_symbols: List[str],
        setting: dict,
    ) -> None:
        """Constructor"""
        self.strategy_engine: "StrategyEngine" = strategy_engine
        self.strategy_name: str = strategy_name
        self.vt_symbols: List[str] = vt_symbols

        # State control variable
        self.inited: bool = False
        self.trading: bool = False

        # Position data dictionary
        self.pos_data: Dict[str, int] = defaultdict(int)  # 实际持仓
        self.target_data: Dict[str, int] = defaultdict(int)  # 目标持仓

        # Delegated cache container
        self.orders: Dict[str, OrderData] = {}
        self.active_orderids: Set[str] = set()

        # Copy the list of variable names and insert the default variable contents
        self.variables: list = copy(self.variables)
        self.variables.insert(0, "inited")
        self.variables.insert(1, "trading")
        self.variables.insert(2, "pos_data")
        self.variables.insert(3, "target_data")

        # Setting Strategy Parameters
        self.update_setting(setting)

    def update_setting(self, setting: dict) -> None:
        """Setting the strategy parameters"""
        for name in self.parameters:
            if name in setting:
                setattr(self, name, setting[name])

    @classmethod
    def get_class_parameters(cls) -> dict:
        """Look up the default parameters of the strategy"""
        class_parameters: dict = {}
        for name in cls.parameters:
            class_parameters[name] = getattr(cls, name)
        return class_parameters

    def get_parameters(self) -> dict:
        """Query strategy parameters"""
        strategy_parameters: dict = {}
        for name in self.parameters:
            strategy_parameters[name] = getattr(self, name)
        return strategy_parameters

    def get_variables(self) -> dict:
        """Query strategy variables"""
        strategy_variables: dict = {}
        for name in self.variables:
            strategy_variables[name] = getattr(self, name)
        return strategy_variables

    def get_data(self) -> dict:
        """Query strategy status data"""
        strategy_data: dict = {
            "strategy_name": self.strategy_name,
            "vt_symbols": self.vt_symbols,
            "class_name": self.__class__.__name__,
            "author": self.author,
            "parameters": self.get_parameters(),
            "variables": self.get_variables(),
        }
        return strategy_data

    @virtual
    def on_init(self) -> None:
        """Strategy initialization callback""""
        pass

    @virtual
    def on_start(self) -> None:
        """Strategy startup callback"""
        pass

    @virtual
    def on_stop(self) -> None:
        """Strategy stop callback"""
        pass

    @virtual
    def on_tick(self, tick: TickData) -> None:
        """Strategy tick callback"""
        pass

    @virtual
    def on_bars(self, bars: Dict[str, BarData]) -> None:
        """Bar callback"""
        pass

    def update_trade(self, trade: TradeData) -> None:
        """Trade data update"""
        if trade.direction == Direction.LONG:
            self.pos_data[trade.vt_symbol] += trade.volume
        else:
            self.pos_data[trade.vt_symbol] -= trade.volume

    def update_order(self, order: OrderData) -> None:
        """Order data update"""
        self.orders[order.vt_orderid] = order

        if not order.is_active() and order.vt_orderid in self.active_orderids:
            self.active_orderids.remove(order.vt_orderid)

    def buy(
        self,
        vt_symbol: str,
        price: float,
        volume: float,
        lock: bool = False,
        net: bool = False,
    ) -> List[str]:
        """Open buy"""
        return self.send_order(
            vt_symbol, Direction.LONG, Offset.OPEN, price, volume, lock, net
        )

    def sell(
        self,
        vt_symbol: str,
        price: float,
        volume: float,
        lock: bool = False,
        net: bool = False,
    ) -> List[str]:
        """Sell buy"""
        return self.send_order(
            vt_symbol, Direction.SHORT, Offset.CLOSE, price, volume, lock, net
        )

    def short(
        self,
        vt_symbol: str,
        price: float,
        volume: float,
        lock: bool = False,
        net: bool = False,
    ) -> List[str]:
        """Open short"""
        return self.send_order(
            vt_symbol, Direction.SHORT, Offset.OPEN, price, volume, lock, net
        )

    def cover(
        self,
        vt_symbol: str,
        price: float,
        volume: float,
        lock: bool = False,
        net: bool = False,
    ) -> List[str]:
        """Cover short"""
        return self.send_order(
            vt_symbol, Direction.LONG, Offset.CLOSE, price, volume, lock, net
        )

    def send_order(
        self,
        vt_symbol: str,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        lock: bool = False,
        net: bool = False,
    ) -> List[str]:
        """Send a new order"""
        if self.trading:
            vt_orderids: list = self.strategy_engine.send_order(
                self, vt_symbol, direction, offset, price, volume, lock, net
            )

            for vt_orderid in vt_orderids:
                self.active_orderids.add(vt_orderid)

            return vt_orderids
        else:
            return []

    def cancel_order(self, vt_orderid: str) -> None:
        """Cancel order"""
        if self.trading:
            self.strategy_engine.cancel_order(self, vt_orderid)

    def cancel_all(self) -> None:
        """Cancel all orders"""
        for vt_orderid in list(self.active_orderids):
            self.cancel_order(vt_orderid)

    def get_pos(self, vt_symbol: str) -> int:
        """Check Current Position"""
        return self.pos_data.get(vt_symbol, 0)

    def get_target(self, vt_symbol: str) -> int:
        """Check Target Position"""
        return self.target_data[vt_symbol]

    def set_target(self, vt_symbol: str, target: int) -> None:
        """Setting Target Positions"""
        self.target_data[vt_symbol] = target

    def rebalance_portfolio(self, bars: Dict[str, BarData]) -> None:
        """Execute a position trade based on the target"""
        self.cancel_all()

        # Issues orders only for contracts with current K-slices.
        for vt_symbol, bar in bars.items():
            # Calculate position spreads
            target: int = self.get_target(vt_symbol)
            pos: int = self.get_pos(vt_symbol)
            diff: int = target - pos

            # Long
            if diff > 0:
                # Calculate the long order price
                order_price: float = self.calculate_price(
                    vt_symbol, Direction.LONG, bar.close_price
                )

                # Calculate the cover and buy volume
                cover_volume: int = 0
                buy_volume: int = 0

                if pos < 0:
                    cover_volume = min(diff, abs(pos))
                    buy_volume = diff - cover_volume
                else:
                    buy_volume = diff

                # Issuance of correspondent orders
                if cover_volume:
                    self.cover(vt_symbol, order_price, cover_volume)

                if buy_volume:
                    self.buy(vt_symbol, order_price, buy_volume)
            # Short
            elif diff < 0:
                # Calculate Short Order Price
                order_price: float = self.calculate_price(
                    vt_symbol, Direction.SHORT, bar.close_price
                )

                # Calculate the sell and short volume
                sell_volume: int = 0
                short_volume: int = 0

                if pos > 0:
                    sell_volume = min(abs(diff), pos)
                    short_volume = abs(diff) - sell_volume
                else:
                    short_volume = abs(diff)

                # Issuance of correspondent orders
                if sell_volume:
                    self.sell(vt_symbol, order_price, sell_volume)

                if short_volume:
                    self.short(vt_symbol, order_price, short_volume)

    @virtual
    def calculate_price(
        self, vt_symbol: str, direction: Direction, reference: float
    ) -> float:
        """Calculation of transfer order price (supports on-demand reloading implementation)"""
        return reference

    def get_order(self, vt_orderid: str) -> Optional[OrderData]:
        """Query proxy data"""
        return self.orders.get(vt_orderid, None)

    def get_all_active_orderids(self) -> List[OrderData]:
        """Get the orders for all active states"""
        return list(self.active_orderids)

    def write_log(self, msg: str) -> None:
        """Logs of the strategy"""
        self.strategy_engine.write_log(msg, self)

    def get_engine_type(self) -> EngineType:
        """Query Engine Type"""
        return self.strategy_engine.get_engine_type()

    def get_pricetick(self, vt_symbol: str) -> float:
        """Query Contract Minimum Price Jump"""
        return self.strategy_engine.get_pricetick(self, vt_symbol)

    def get_size(self, vt_symbol: str) -> int:
        """Query Contract Multiplier"""
        return self.strategy_engine.get_size(self, vt_symbol)

    def load_bars(self, days: int, interval: Interval = Interval.MINUTE) -> None:
        """Load historical K-line data to perform initialization"""
        self.strategy_engine.load_bars(self, days, interval)

    def put_event(self) -> None:
        """Push strategy data update events"""
        if self.inited:
            self.strategy_engine.put_strategy_event(self)

    def send_email(self, msg: str) -> None:
        """Send e-mail message"""
        if self.inited:
            self.strategy_engine.send_email(msg, self)

    def sync_data(self) -> None:
        """Synchronizing Strategy Status Data to Files"""
        if self.trading:
            self.strategy_engine.sync_strategy_data(self)
