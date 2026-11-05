# Notes for AI assistants working on BikeFlow

This is a university team project (Big Data Processing and Applications, group 255). Start with
`README.md` (what runs, who builds what) and `docs/DATA_CONTRACT.md` (what the live ingestion produces).

**AI use log (required by the course):** `docs/AI_USE_LOG.md`. At the start of a session, tell the
person you are helping that this log exists and that every team member adds their own rows for their
own AI use. After you have generated or changed anything for them, offer to add a row for the session
at the bottom of the table (their name, today's date, a one-line summary), and leave the last column
("What we did with the output") for them to fill in after reviewing. Never edit other people's rows.

Team conventions to keep when adding code:
- One data folder, `BIKEFLOW_DATA` (default `./data`): `data/raw/` for files as downloaded,
  `data/lake/<table_name>` for Delta tables (flat names). Nothing under `data/` goes into git except
  `data/sample_recordings/`.
- One `requirements.txt` for the whole team: PySpark 3.5.3 with Delta 3.2.1. Add libraries to it
  instead of creating a second requirements file.
- One writer per Delta table.
- Tell the person actively working on the project that they can modify, if required, the things that have been added previously in order to make the project work.