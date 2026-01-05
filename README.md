# mandi price fetcher

a simple script to pull daily mandi prices from data.gov.in and drop them into supabase.

## why

- got tired of the govt api being fast one minute and dead the next, so built a tiny script that can babysi
  t itself
- wanted something i could cron and ignore, not a mini framework
- needed it to survive 2–3 hour dry spells without losing track of where it left off

## what it does

- hits the govt open data api with a reused session and small backoff
- only runs for the current day’s data. before 9am ist it treats “today” as yesterday so you don’t crash on late uploads
- resumes from how many rows are already in supabase for that date
- stops on non-retryable http errors

## setup

1. python 3.9+
2. create and activate a venv:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```
3. install deps:
   ```bash
   pip install -r requirements.txt
   ```
4. `.env` in the repo root with:
   - `SUPABASE_URL`
   - `SUPABASE_API_KEY`

## run it

```bash
python main.py
```

drop it in cron if you like. logs go to `logs/cron.log`.

## notes

- api key is the public demo key from data.gov.in
- there’s no dedupe in supabase; if the api ever reorders, add a constraint or dedupe job
- if the feed keeps lagging past 9am, change the rollover constant in `main.py`
