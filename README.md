# burndown

A personal sprint board with a Telegram bot that keeps you accountable. It uses only the Python standard library, stores everything in one SQLite file, and runs as one container.

## Run

```bash
cp .env.example .env     # set BOARD_PASSWORD, TELEGRAM_BOT_TOKEN, BOARD_URL
docker compose up -d --build
# or without Docker:
set -a; . ./.env; set +a; python3 server.py
```

Open http://<host>:8088. Then send `/start` to your bot in Telegram. The first chat that does this gets linked, and every other chat is ignored.

## Flow

1. **Plan** (every two weeks, about 45 min): create a sprint, set a goal, add your commitments with due dates.
2. **Work:** drag cards across To do → In progress → Blocked → Done, or use `/doing 12` and `/done 12` in Telegram.
3. **Daily alerts** (default 09:00 and 21:00): what's remaining, what's due today, what slipped.
4. **Slipped?** The card glows red and appears in every digest until you **Spill** it (with a category and reason) or finish it.
5. **Mid-sprint check:** a reminder on the middle Saturday.
6. **Close sprint:** every unfinished item needs a decision (next sprint, backlog, or drop) and a reason.

## Rules (enforced by the server)

- Moving a deadline later, or pulling an open item out of a running sprint, counts as a spill and needs a reason.
- The dates of a running sprint are frozen.
- Items with history can't be deleted, only dropped with a reason.
- The ledger is append-only.

## Telegram commands

`/today` `/overdue` `/sprint` `/doing <id>` `/done <id>` `/help`

## Backup

`data/burndown.db` holds everything. Copy it, or add it to your existing backup job.
