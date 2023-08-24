# Version 1.0.6

1. Add engine type query function get_engine_type to policy templates.

# Version 1.0.5

1. Add backtest burst check
2. Strategy template to add contract multiplier query function get_size
3. Load daily and hourly data without using segmented loading


# Version 1.0.4

1. Fix the problem that defaultdict becomes dict when pos_data/target_data is restored from cache file.
2. Change to use OffsetConverter provided by OmsEngine.
3. Add log output when querying historical data.


# Version 1.0.3

1. Add position target transfer trading mode to the portfolio strategy template.
2. Fix the error of backtesting profit/loss calculation caused by missing bar quotes in some cases.

# Version 1.0.2

1. Use zoneinfo to replace pytz library
2. Adjust the installation script setup.cfg to add Python version restriction.

# Version 1.0.1

1. Change the icon file information of the module to the full path string.
2. Change to use PySide6 style signal QtCore.