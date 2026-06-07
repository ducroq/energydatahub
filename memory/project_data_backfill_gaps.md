---
name: Data backfill capabilities
description: Which collectors can backfill historical data vs which lose data permanently if not collected daily
type: project
---

Analyzed all collectors for backfill capability (2026-03-30):

**Can backfill** (historical API support): ENTSO-E family, Elspot/Nord Pool, ENTSOG gas flows, GIE gas storage, NED production, TenneT imbalance, Luchtmeetnet

**Permanently lost if missed**: Weather forecasts (Google, Open-Meteo solar/wind/demand), EnergyZero day-ahead prices

**Partially recoverable**: Market proxies (daily close via yfinance), EPEX (7-30 days history)

**Why:** Needed during March 2026 Actions outage. Weather forecasts and EnergyZero prices are forward-looking only — no archive of past forecasts exists.

**How to apply:** If collection is down, prioritize manual runs to preserve weather/price forecasts. ENTSO-E and grid data can always be backfilled later.
