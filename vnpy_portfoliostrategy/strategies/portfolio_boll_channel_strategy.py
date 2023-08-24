from typing import List, Dict
from datetime import datetime

from vnpy.trader.utility import ArrayManager, Interval
from vnpy.trader.object import TickData, BarData

from vnpy_portfoliostrategy import StrategyTemplate, StrategyEngine
from vnpy_portfoliostrategy.utility import PortfolioBarGenerator


class PortfolioBollChannelStrategy(StrategyTemplate):
    """Combination Bollinger Band Channel Strategy"""

    author = "Trader in Python."

    boll_window = 18
    boll_dev = 3.4
    cci_window = 10
    atr_window = 30
    sl_multiplier = 5.2
    fixed_size = 1
    price_add = 5

    parameters = [
        "boll_window",
        "boll_dev",
        "cci_window",
        "atr_window",
        "sl_multiplier",
        "fixed_size",
        "price_add",
    ]
    variables = []

    def __init__(
        self,
        strategy_engine: StrategyEngine,
        strategy_name: str,
        vt_symbols: List[str],
        setting: dict,
    ) -> None:
        """Constructor"""
        super().__init__(strategy_engine, strategy_name, vt_symbols, setting)

        self.boll_up: Dict[str, float] = {}
        self.boll_down: Dict[str, float] = {}
        self.cci_value: Dict[str, float] = {}
        self.atr_value: Dict[str, float] = {}
        self.intra_trade_high: Dict[str, float] = {}
        self.intra_trade_low: Dict[str, float] = {}

        self.targets: Dict[str, int] = {}
        self.last_tick_time: datetime = None

        # Getting Contract Information
        self.ams: Dict[str, ArrayManager] = {}
        for vt_symbol in self.vt_symbols:
            self.ams[vt_symbol] = ArrayManager()
            self.targets[vt_symbol] = 0

        self.pbg = PortfolioBarGenerator(
            self.on_bars, 2, self.on_2hour_bars, Interval.HOUR
        )

    def on_init(self) -> None:
        """Strategy initialization callback"""
        self.write_log("Strategy initialized")

        self.load_bars(10)

    def on_start(self) -> None:
        """Strategy startup callback"""
        self.write_log("Strategy activated")

    def on_stop(self) -> None:
        """Strategy stop callback"""
        self.write_log("Strategy stopped")

    def on_tick(self, tick: TickData) -> None:
        """Strategy tick callback"""
        self.pbg.update_tick(tick)

    def on_bars(self, bars: Dict[str, BarData]) -> None:
        """Bar callback"""
        self.pbg.update_bars(bars)

    def on_2hour_bars(self, bars: Dict[str, BarData]) -> None:
        """2-hour K-line retracement"""
        self.cancel_all()

        # Updating to a cached sequence
        for vt_symbol, bar in bars.items():
            am: ArrayManager = self.ams[vt_symbol]
            am.update_bar(bar)

        for vt_symbol, bar in bars.items():
            am: ArrayManager = self.ams[vt_symbol]
            if not am.inited:
                return

            self.boll_up[vt_symbol], self.boll_down[vt_symbol] = am.boll(
                self.boll_window, self.boll_dev
            )
            self.cci_value[vt_symbol] = am.cci(self.cci_window)
            self.atr_value[vt_symbol] = am.atr(self.atr_window)

            # Calculate target position
            current_pos = self.get_pos(vt_symbol)
            if current_pos == 0:
                self.intra_trade_high[vt_symbol] = bar.high_price
                self.intra_trade_low[vt_symbol] = bar.low_price

                if self.cci_value[vt_symbol] > 0:
                    self.targets[vt_symbol] = self.fixed_size
                elif self.cci_value[vt_symbol] < 0:
                    self.targets[vt_symbol] = -self.fixed_size

            elif current_pos > 0:
                self.intra_trade_high[vt_symbol] = max(
                    self.intra_trade_high[vt_symbol], bar.high_price
                )
                self.intra_trade_low[vt_symbol] = bar.low_price

                long_stop = (
                    self.intra_trade_high[vt_symbol]
                    - self.atr_value[vt_symbol] * self.sl_multiplier
                )

                if bar.close_price <= long_stop:
                    self.targets[vt_symbol] = 0

            elif current_pos < 0:
                self.intra_trade_low[vt_symbol] = min(
                    self.intra_trade_low[vt_symbol], bar.low_price
                )
                self.intra_trade_high[vt_symbol] = bar.high_price

                short_stop = (
                    self.intra_trade_low[vt_symbol]
                    + self.atr_value[vt_symbol] * self.sl_multiplier
                )

                if bar.close_price >= short_stop:
                    self.targets[vt_symbol] = 0

        # Order based on target positions
        for vt_symbol in self.vt_symbols:
            target_pos = self.targets[vt_symbol]
            current_pos = self.get_pos(vt_symbol)

            pos_diff = target_pos - current_pos
            volume = abs(pos_diff)
            bar = bars[vt_symbol]
            boll_up = self.boll_up[vt_symbol]
            boll_down = self.boll_down[vt_symbol]

            if pos_diff > 0:
                price = bar.close_price + self.price_add

                if current_pos < 0:
                    self.cover(vt_symbol, price, volume)
                else:
                    self.buy(vt_symbol, boll_up, volume)

            elif pos_diff < 0:
                price = bar.close_price - self.price_add

                if current_pos > 0:
                    self.sell(vt_symbol, price, volume)
                else:
                    self.short(vt_symbol, boll_down, volume)

        # Push interface update
        self.put_event()
