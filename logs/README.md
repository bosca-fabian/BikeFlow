# Logs

Output of the streaming job and of the ingestion services when they are run from the
command line, for example:

```bash
bikeflow-stream run --log-file logs/stream.log
bikeflow-ingest produce > logs/producer.log 2>&1
```

Everything in this folder except `.gitkeep` is ignored by git (see `.gitignore`): logs
belong to a run, not to the repository. Inside Docker, use `docker compose logs -f
stream` instead, which is the same output.
