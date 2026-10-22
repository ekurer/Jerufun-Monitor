# Jerufun-Monitor (Ephraim's instance)

Personal deployment of the JeruFun bike-station monitor for Jerusalem.

## Credit

This project is based on the open-source project
[Jerufun-Monitor](https://github.com/yuvalreisner/Jerufun-Monitor) by **Yuval Reisner**.
All core collection logic (collector.py, config.py, db.py) comes from that repository.

This fork adds `runner.py` (Railway runner with retries, a health endpoint,
a raw DB download, and a live Hebrew dashboard at `/dashboard` showing the
station map and fill-over-time graphs).

Map samples are stored as change events (plus one daily anchor) in `map_events`, with the latest state in `map_current`, to keep the database small.
