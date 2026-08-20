# cloud-project

Tooling for recording and analyzing TikTok Live sessions: a terminal recorder that
captures everything the stream exposes, and a single-file browser app for reading
the results back.

## Files

| File | What it is |
| --- | --- |
| `recorder.py` | TikTok Live chat/event recorder (terminal edition, v0.8). Connects to a live via the TikTokLive library and writes chat, events, bot decisions, and a badge probe to timestamped files. Also sends ntfy.sh push notifications (start ping, live ping, end-of-stream summary with the files attached). |
| `events-analyzer (1).html` | Standalone browser analyzer — open it directly, no server or build step. Drop one or more recorder CSVs on it to get a stream pulse, tap leaderboard, new followers, shares, Q&A questions, and per-user lookup. |
| `sample-chat.csv` | Sample chat capture, 120 rows — `time,username,message`. |
| `sample-events.csv` | Sample event capture, 901 rows — `time,username,event,detail` (likes, joins, follows, shares, gifts). |
| `sample-botlog.txt` | Sample bot dry-run log — the messages the bot *would* have posted, with the trigger for each, plus a session summary. |
| `sample-badges.txt` | Sample badge probe — raw badge fields per unique user, used to identify moderators (`ADMIN`) and fan-club members (`FANS`). |

The four `sample-*` files are fixtures for the analyzer; drop them on the HTML page
to see it working without recording a live first.

## Running the recorder

Requires **Python 3.12 or 3.13** — 3.14 breaks the TikTokLive library's protobuf
schemas, which silently disables gifts and most event types.

```bash
pip install TikTokLive
python recorder.py
```

Set `NTFY_TOPIC` near the top of `recorder.py` to enable push notifications; it
ships as `"CHANGE-ME"` and pushes are skipped until it is changed.

Each session writes four files sharing one timestamp:

```
tiktok-chat_<user>_<stamp>.csv     time,username,message
tiktok-events_<user>_<stamp>.csv   time,username,event,detail
tiktok-botlog_<user>_<stamp>.txt   bot dry-run decisions
badges-sample_<user>_<stamp>.txt   raw badge fields per unique user
```

These are gitignored — only the checked-in `sample-*` files are tracked.

## Bot status

The bot brain (milestone thank-yous, moderator/fan-club greetings) runs in
**dry-run only**. It writes `WOULD POST: ...` lines to the botlog and does not
post anything to the live. Volume limits: 20s minimum interval, queue of 40, hard
cap of 250 messages per session, with greetings exempt from the cap.
