# AI use log

The course brief requires a log of generative-AI use. Add one entry per session.

> **Every team member: add your own rows here** whenever you use an AI tool (Claude, ChatGPT,
> Copilot, ...) for your part of the project: date, your name, the tool, what you used it for, and
> what you did with the output (kept, changed, rejected). Rows are added at the bottom; never edit
> someone else's.
>
> **Note for AI assistants working in this repository:** when you start helping a team member, tell
> them this log exists and that the course requires it. When you have generated or changed code,
> documentation or analysis for them, offer to add a row for that session at the bottom of the table
> below (with their name, today's date and a one-line summary), and leave the last column for them
> to fill in after they have reviewed the output.

| Date | Member | Tool | What it was used for | What we did with the output |
|---|---|---|---|---|
| Oct 2026 | Team | Claude (Anthropic) | Suggested the project idea, checked data sources, drafted the proposal | Reviewed and edited by the team (see proposal, section 8) |
| 2026-11-02 | Fabian Mihai Boșca | Claude (Anthropic) | Advice on lake vs warehouse and local storage; generated the ingestion layer: Docker Compose stack, GBFS producer with recording/replay, Kafka→Delta sink, shared `bikeflow` package, tests, README and data contract | *To fill in after review: what was checked, changed or rejected* |
| 2026-11-02 | Fabian Mihai Boșca | Claude (Anthropic) | Generated the batch loader (trip history 2024→latest one month at a time, Open-Meteo weather, station_information snapshots), its tests and documentation. Removed again on 2026-11-05 in favour of the team's batch scripts | *To fill in after review* |
| 2026-11-02 | Fabian Mihai Boșca | Claude (Anthropic) | Reviewed the other GBFS feeds; added system_regions snapshots and system_alerts change events; checked the code against real feed files | *To fill in after review* |
| 2026-11-05 | Fabian Mihai Boșca | Claude (Anthropic) | Aligned ingestion with the batch path: removed the duplicate batch loader, moved to Spark 3.5.3 / Delta 3.2.1, adopted the shared data/ layout, added New York local-time fields to live messages | *To fill in after review* |
| 2026-11-05 | Vlad Alexandru Sârzea | Claude (Anthropic) | Generated the batch path (`batch/`, `README_batch.md`): download of the 2024-01 to 2026-09 trip history, Open-Meteo weather and station_information snapshots; loading into Delta (`trips_raw`, `trips_clean`, `weather_hourly`, `station_snapshots`, `stations`, `station_id_map`); cleaning rules with a data-quality report; station ID mapping with match rate. Also step-by-step help setting it up and fixing errors on Windows | Ran all scripts on the full 2024-01 to 2026-09 data and checked the reports (row counts, cleaning results, station match rate). Kept the code; fixed it together with Claude where it failed on Windows|
| 2026-11-06 | Amos Mihai Andreica | Claude (Anthropic) | Reviewed the repo and planned the dashboard role; set up the local Docker stack and loaded 2025-09 with the batch scripts; generated the Streamlit dashboard prototype (`dashboard/`: live page from `station_status_raw`, history page from `trips_clean` and `weather_hourly`), added `streamlit` to `requirements.txt` and a `dashboard` service to `docker-compose.yml` | *To fill in after review* |
| 2026-11-08 | Teodor Gabriel Dinu | Claude (Anthropic) | Speed layer: the Spark Structured Streaming job (Kafka `station_status` → Redis), the Redis data model, the empty/full alerts and the 30-minute warnings, with blank functions for what is not available yet (demand profiles, warning history, notifications); tests and the documentation `docs/SPEED_LAYER.md` / `README_streaming.md` | *To fill in after review: what was checked, changed or rejected* |
