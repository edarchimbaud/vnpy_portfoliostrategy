import importlib
import glob
import traceback
from collections import defaultdict
from pathlib import Path
from types import ModuleType
from typing import Dict, List, Set, Tuple, Type, Any, Callable, Optional
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from vnpy.event import Event, EventEngine
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.object import (
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    HistoryRequest,
    LogData,
    TickData,
    OrderData,
    TradeData,
    BarData,
    ContractData,
)
from vnpy.trader.event import EVENT_TICK, EVENT_ORDER, EVENT_TRADE
from vnpy.trader.constant import Direction, OrderType, Interval, Exchange, Offset
from vnpy.trader.utility import load_json, save_json, extract_vt_symbol, round_to
from vnpy.trader.datafeed import BaseDatafeed, get_datafeed
from vnpy.trader.database import BaseDatabase, get_database, DB_TZ

from .base import APP_NAME, EVENT_PORTFOLIO_LOG, EVENT_PORTFOLIO_STRATEGY, EngineType
from .template import StrategyTemplate


class StrategyEngine(BaseEngine):
    """Portfolio Strategy Engine"""

    engine_type: EngineType = EngineType.LIVE

    setting_filename: str = "portfolio_strategy_setting.json"
    data_filename: str = "portfolio_strategy_data.json"

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        """"""
        super().__init__(main_engine, event_engine, APP_NAME)

        self.strategy_data: Dict[str, Dict] = {}

        self.classes: Dict[str, Type[StrategyTemplate]] = {}
        self.strategies: Dict[str, StrategyTemplate] = {}

        self.symbol_strategy_map: Dict[str, List[StrategyTemplate]] = defaultdict(list)
        self.orderid_strategy_map: Dict[str, StrategyTemplate] = {}

        self.init_executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=1)

        self.vt_tradeids: Set[str] = set()

        # Database and data services
        self.database: BaseDatabase = get_database()
        self.datafeed: BaseDatafeed = get_datafeed()

    def init_engine(self) -> None:
        """Initializing the engine"""
        self.init_datafeed()
        self.load_strategy_class()
        self.load_strategy_setting()
        self.load_strategy_data()
        self.register_event()
        self.write_log("Portfolio strategy engine initialized successfully")

    def close(self) -> None:
        """Close"""
        self.stop_all_strategies()

    def register_event(self) -> None:
        """Register Event Engine"""
        self.event_engine.register(EVENT_TICK, self.process_tick_event)
        self.event_engine.register(EVENT_ORDER, self.process_order_event)
        self.event_engine.register(EVENT_TRADE, self.process_trade_event)

    def init_datafeed(self) -> None:
        """Initializing Data Services"""
        result: bool = self.datafeed.init(self.write_log)
        if result:
            self.write_log("Data service initialization successful.")

    def query_bar_from_datafeed(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime,
    ) -> List[BarData]:
        """Access to historical data through data services"""
        req: HistoryRequest = HistoryRequest(
            symbol=symbol, exchange=exchange, interval=interval, start=start, end=end
        )
        data: List[BarData] = self.datafeed.query_bar_history(req, self.write_log)
        return data

    def process_tick_event(self, event: Event) -> None:
        """Market Data Push"""
        tick: TickData = event.data

        strategies: list = self.symbol_strategy_map[tick.vt_symbol]
        if not strategies:
            return

        for strategy in strategies:
            if strategy.inited:
                self.call_strategy_func(strategy, strategy.on_tick, tick)

    def process_order_event(self, event: Event) -> None:
        """Order Data Push"""
        order: OrderData = event.data

        strategy: Optional[StrategyTemplate] = self.orderid_strategy_map.get(
            order.vt_orderid, None
        )
        if not strategy:
            return

        self.call_strategy_func(strategy, strategy.update_order, order)

    def process_trade_event(self, event: Event) -> None:
        """Trade Data Push"""
        trade: TradeData = event.data

        # Filter duplicate deal pushes
        if trade.vt_tradeid in self.vt_tradeids:
            return
        self.vt_tradeids.add(trade.vt_tradeid)

        # Push to strategy
        strategy: Optional[StrategyTemplate] = self.orderid_strategy_map.get(
            trade.vt_orderid, None
        )
        if not strategy:
            return

        self.call_strategy_func(strategy, strategy.update_trade, trade)

    def send_order(
        self,
        strategy: StrategyTemplate,
        vt_symbol: str,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        lock: bool,
        net: bool,
    ) -> list:
        """Send an order"""
        contract: Optional[ContractData] = self.main_engine.get_contract(vt_symbol)
        if not contract:
            self.write_log(f"Order failed, contract not found: {vt_symbol}", strategy)
            return ""

        price: float = round_to(price, contract.pricetick)
        volume: float = round_to(volume, contract.min_volume)

        original_req: OrderRequest = OrderRequest(
            symbol=contract.symbol,
            exchange=contract.exchange,
            direction=direction,
            offset=offset,
            type=OrderType.LIMIT,
            price=price,
            volume=volume,
            reference=f"{APP_NAME}_{strategy.strategy_name}",
        )

        req_list: List[OrderRequest] = self.main_engine.convert_order_request(
            original_req, contract.gateway_name, lock, net
        )

        vt_orderids: list = []

        for req in req_list:
            vt_orderid: str = self.main_engine.send_order(req, contract.gateway_name)

            if not vt_orderid:
                continue

            vt_orderids.append(vt_orderid)

            self.main_engine.update_order_request(
                req, vt_orderid, contract.gateway_name
            )

            self.orderid_strategy_map[vt_orderid] = strategy

        return vt_orderids

    def cancel_order(self, strategy: StrategyTemplate, vt_orderid: str) -> None:
        """Order Cancellation"""
        order: Optional[OrderData] = self.main_engine.get_order(vt_orderid)
        if not order:
            self.write_log(
                f"Cancellation failed, commission not found {vt_orderid}", strategy
            )
            return

        req: CancelRequest = order.create_cancel_request()
        self.main_engine.cancel_order(req, order.gateway_name)

    def get_engine_type(self) -> EngineType:
        """Get Engine Type"""
        return self.engine_type

    def get_pricetick(self, strategy: StrategyTemplate, vt_symbol: str) -> float:
        """Get contract price jumps"""
        contract: Optional[ContractData] = self.main_engine.get_contract(vt_symbol)

        if contract:
            return contract.pricetick
        else:
            return None

    def get_size(self, strategy: StrategyTemplate, vt_symbol: str) -> int:
        """Get contract multiplier"""
        contract: Optional[ContractData] = self.main_engine.get_contract(vt_symbol)

        if contract:
            return contract.size
        else:
            return None

    def load_bars(
        self, strategy: StrategyTemplate, days: int, interval: Interval
    ) -> None:
        """Load historical data"""
        vt_symbols: list = strategy.vt_symbols
        dts: Set[datetime] = set()
        history_data: Dict[Tuple, BarData] = {}

        # Access to historical data through interfaces, data services, databases
        for vt_symbol in vt_symbols:
            data: List[BarData] = self.load_bar(vt_symbol, days, interval)

            for bar in data:
                dts.add(bar.datetime)
                history_data[(bar.datetime, vt_symbol)] = bar

        dts: list = list(dts)
        dts.sort()

        bars: dict = {}

        for dt in dts:
            for vt_symbol in vt_symbols:
                bar: Optional[BarData] = history_data.get((dt, vt_symbol), None)

                # If historical data is obtained for the time specified in the contract, it is cached in the bars dictionary.
                if bar:
                    bars[vt_symbol] = bar
                # If you can't get it, but there is already a cache of contract data in the bars dictionary, use the previous data to fill it.
                elif vt_symbol in bars:
                    old_bar: BarData = bars[vt_symbol]

                    bar = BarData(
                        symbol=old_bar.symbol,
                        exchange=old_bar.exchange,
                        datetime=dt,
                        open_price=old_bar.close_price,
                        high_price=old_bar.close_price,
                        low_price=old_bar.close_price,
                        close_price=old_bar.close_price,
                        gateway_name=old_bar.gateway_name,
                    )
                    bars[vt_symbol] = bar

            self.call_strategy_func(strategy, strategy.on_bars, bars)

    def load_bar(self, vt_symbol: str, days: int, interval: Interval) -> List[BarData]:
        """Load Individual Contract Historical Data"""
        symbol, exchange = extract_vt_symbol(vt_symbol)
        end: datetime = datetime.now(DB_TZ)
        start: datetime = end - timedelta(days)
        contract: Optional[ContractData] = self.main_engine.get_contract(vt_symbol)
        data: List[BarData]

        # Getting historical data through the interface
        if contract and contract.history_data:
            req: HistoryRequest = HistoryRequest(
                symbol=symbol,
                exchange=exchange,
                interval=interval,
                start=start,
                end=end,
            )
            data = self.main_engine.query_history(req, contract.gateway_name)

        # Access to historical data through data services
        else:
            data = self.query_bar_from_datafeed(symbol, exchange, interval, start, end)

        # Getting data through the database
        if not data:
            data = self.database.load_bar_data(
                symbol=symbol,
                exchange=exchange,
                interval=interval,
                start=start,
                end=end,
            )

        return data

    def call_strategy_func(
        self, strategy: StrategyTemplate, func: Callable, params: Any = None
    ) -> None:
        """Call strategy function"""
        try:
            if params:
                func(params)
            else:
                func()
        except Exception:
            strategy.trading = False
            strategy.inited = False

            msg: str = f"Trigger exception stopped \n{traceback.format_exc()}"
            self.write_log(msg, strategy)

    def add_strategy(
        self, class_name: str, strategy_name: str, vt_symbols: list, setting: dict
    ) -> None:
        """Adding a Strategy Example"""
        if strategy_name in self.strategies:
            self.write_log(
                f"Failed to create strategy, there is a rename {strategy_name}"
            )
            return

        strategy_class: Optional[StrategyTemplate] = self.classes.get(class_name, None)
        if not strategy_class:
            self.write_log(
                f"Failed to create strategy, strategy class {class_name} not found"
            )
            return

        strategy: StrategyTemplate = strategy_class(
            self, strategy_name, vt_symbols, setting
        )
        self.strategies[strategy_name] = strategy

        for vt_symbol in vt_symbols:
            strategies: list = self.symbol_strategy_map[vt_symbol]
            strategies.append(strategy)

        self.save_strategy_setting()
        self.put_strategy_event(strategy)

    def init_strategy(self, strategy_name: str) -> None:
        """Initialization strategy"""
        self.init_executor.submit(self._init_strategy, strategy_name)

    def _init_strategy(self, strategy_name: str) -> None:
        """Initialization strategy"""
        strategy: StrategyTemplate = self.strategies[strategy_name]

        if strategy.inited:
            self.write_log(
                f"{strategy_name} has completed initialization, repeat operations are prohibited"
            )
            return

        self.write_log(f"{strategy_name} start performing initialization")

        # Calling the strategy on_init function
        self.call_strategy_func(strategy, strategy.on_init)

        # Restore strategy state
        data: Optional[dict] = self.strategy_data.get(strategy_name, None)
        if data:
            for name in strategy.variables:
                value: Optional[Any] = data.get(name, None)
                if value is None:
                    continue

                # For position and target data dictionaries, the defaultdict needs to be updated using dict.update
                if name in {"pos_data", "target_data"}:
                    strategy_data = getattr(strategy, name)
                    strategy_data.update(value)
                # For other int/float/str/bool fields, you can assign values directly.
                else:
                    setattr(strategy, name, value)

        # Subscribe to Quotes
        for vt_symbol in strategy.vt_symbols:
            contract: Optional[ContractData] = self.main_engine.get_contract(vt_symbol)
            if contract:
                req: SubscribeRequest = SubscribeRequest(
                    symbol=contract.symbol, exchange=contract.exchange
                )
                self.main_engine.subscribe(req, contract.gateway_name)
            else:
                self.write_log(
                    f"Quote subscription failed, contract {vt_symbol} not found",
                    strategy,
                )

        # Push strategy event notification initialization completion status
        strategy.inited = True
        self.put_strategy_event(strategy)
        self.write_log(f"{strategy_name} initialization complete")

    def start_strategy(self, strategy_name: str) -> None:
        """Start strategy"""
        strategy: StrategyTemplate = self.strategies[strategy_name]
        if not strategy.inited:
            self.write_log(
                f"Strategy {strategy.strategy_name} failed to start, please initialize first"
            )
            return

        if strategy.trading:
            self.write_log(
                f"{strategy_name} has been activated, please do not repeat the operation."
            )
            return

        # Calling the strategy on_start function
        self.call_strategy_func(strategy, strategy.on_start)

        # Push Strategy Event Notification Initiation Completion Status
        strategy.trading = True
        self.put_strategy_event(strategy)

    def stop_strategy(self, strategy_name: str) -> None:
        """Stop strategy"""
        strategy: StrategyTemplate = self.strategies[strategy_name]
        if not strategy.trading:
            return

        # Call the strategy on_stop function
        self.call_strategy_func(strategy, strategy.on_stop)

        # Set the trading state to False
        strategy.trading = False

        # Cancel all trades
        strategy.cancel_all()

        # Synchronize data state
        self.sync_strategy_data(strategy)

        # Push strategy event notification to stop completion state
        self.put_strategy_event(strategy)

    def edit_strategy(self, strategy_name: str, setting: dict) -> None:
        """Editing Strategy Parameters"""
        strategy: StrategyTemplate = self.strategies[strategy_name]
        strategy.update_setting(setting)

        self.save_strategy_setting()
        self.put_strategy_event(strategy)

    def remove_strategy(self, strategy_name: str) -> bool:
        """Removing Strategy Instances"""
        strategy: StrategyTemplate = self.strategies[strategy_name]
        if strategy.trading:
            self.write_log(
                f"Strategy {strategy.strategy_name} removal failed, please stop first"
            )
            return

        for vt_symbol in strategy.vt_symbols:
            strategies: list = self.symbol_strategy_map[vt_symbol]
            strategies.remove(strategy)

        for vt_orderid in strategy.active_orderids:
            if vt_orderid in self.orderid_strategy_map:
                self.orderid_strategy_map.pop(vt_orderid)

        self.strategies.pop(strategy_name)
        self.save_strategy_setting()

        return True

    def load_strategy_class(self) -> None:
        """Loading Strategy Classes"""
        path1: Path = Path(__file__).parent.joinpath("strategies")
        self.load_strategy_class_from_folder(path1, "vnpy_portfoliostrategy.strategies")

        path2: Path = Path.cwd().joinpath("strategies")
        self.load_strategy_class_from_folder(path2, "strategies")

    def load_strategy_class_from_folder(
        self, path: Path, module_name: str = ""
    ) -> None:
        """Loading Strategy Classes from a Specified Folder"""
        for suffix in ["py", "pyd", "so"]:
            pathname: str = str(path.joinpath(f"*.{suffix}"))
            for filepath in glob.glob(pathname):
                stem: str = Path(filepath).stem
                strategy_module_name: str = f"{module_name}.{stem}"
                self.load_strategy_class_from_module(strategy_module_name)

    def load_strategy_class_from_module(self, module_name: str) -> None:
        """Loading Strategy Classes via Strategy Files"""
        try:
            module: ModuleType = importlib.import_module(module_name)

            for name in dir(module):
                value = getattr(module, name)
                if (
                    isinstance(value, type)
                    and issubclass(value, StrategyTemplate)
                    and value is not StrategyTemplate
                ):
                    self.classes[value.__name__] = value
        except:  # noqa
            msg: str = f"Strategy file {module_name} failed to load, triggering an exception:\n{traceback.format_exc()}"
            self.write_log(msg)

    def load_strategy_data(self) -> None:
        """Load strategy data"""
        self.strategy_data = load_json(self.data_filename)

    def sync_strategy_data(self, strategy: StrategyTemplate) -> None:
        """Saving strategy data to a file"""
        data: dict = strategy.get_variables()
        data.pop("inited")  # Do not save strategy state information
        data.pop("trading")

        self.strategy_data[strategy.strategy_name] = data
        save_json(self.data_filename, self.strategy_data)

    def get_all_strategy_class_names(self) -> list:
        """Get all load strategy class names"""
        return list(self.classes.keys())

    def get_strategy_class_parameters(self, class_name: str) -> dict:
        """Getting Strategy Class Parameters"""
        strategy_class: StrategyTemplate = self.classes[class_name]

        parameters: dict = {}
        for name in strategy_class.parameters:
            parameters[name] = getattr(strategy_class, name)

        return parameters

    def get_strategy_parameters(self, strategy_name) -> dict:
        """Getting Strategy Parameters"""
        strategy: StrategyTemplate = self.strategies[strategy_name]
        return strategy.get_parameters()

    def init_all_strategies(self) -> None:
        """Initialize all strategies"""
        for strategy_name in self.strategies.keys():
            self.init_strategy(strategy_name)

    def start_all_strategies(self) -> None:
        """Enable all strategies"""
        for strategy_name in self.strategies.keys():
            self.start_strategy(strategy_name)

    def stop_all_strategies(self) -> None:
        """Stop all strategies"""
        for strategy_name in self.strategies.keys():
            self.stop_strategy(strategy_name)

    def load_strategy_setting(self) -> None:
        """Load Strategy Configuration"""
        strategy_setting: dict = load_json(self.setting_filename)

        for strategy_name, strategy_config in strategy_setting.items():
            self.add_strategy(
                strategy_config["class_name"],
                strategy_name,
                strategy_config["vt_symbols"],
                strategy_config["setting"],
            )

    def save_strategy_setting(self) -> None:
        """Save Strategy Configuration"""
        strategy_setting: dict = {}

        for name, strategy in self.strategies.items():
            strategy_setting[name] = {
                "class_name": strategy.__class__.__name__,
                "vt_symbols": strategy.vt_symbols,
                "setting": strategy.get_parameters(),
            }

        save_json(self.setting_filename, strategy_setting)

    def put_strategy_event(self, strategy: StrategyTemplate) -> None:
        """Push Event Update Strategy Interface"""
        data: dict = strategy.get_data()
        event: Event = Event(EVENT_PORTFOLIO_STRATEGY, data)
        self.event_engine.put(event)

    def write_log(self, msg: str, strategy: StrategyTemplate = None) -> None:
        """Output log"""
        if strategy:
            msg: str = f"{strategy.strategy_name}: {msg}"

        log: LogData = LogData(msg=msg, gateway_name=APP_NAME)
        event: Event = Event(type=EVENT_PORTFOLIO_LOG, data=log)
        self.event_engine.put(event)

    def send_email(self, msg: str, strategy: StrategyTemplate = None) -> None:
        """Send an email"""
        if strategy:
            subject: str = f"{strategy.strategy_name}"
        else:
            subject: str = "Portfolio Strategy Engine"

        self.main_engine.send_email(subject, msg)
