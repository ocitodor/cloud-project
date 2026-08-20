"""
TikTok Live Chat Recorder — terminal edition v0.8
Captures EVERYTHING the TikTokLive library exposes for a live.

v0.8:
  - Thank-yous now state the EXACT cumulative count and use the
    room's word: „Благодаря, {име}, за {брой} тапвания!"
  - VIPS list: important regulars greeted once per session even
    without any badge.
  - Volume ceiling: min interval 20s, queue 40, hard cap 250
    messages/session (greetings exempt).
v0.7:
  - SUPERFAN GREETINGS: users carrying the FANS (Team fan-club)
    badge get greeted once per session — wording approved:
    „Здравей, {име}! 💎 Верен фен от отбора! 🐺". Confirmed from
    a live badge probe: ADMIN badge = moderator (all 5 mods
    matched), FANS badge = Team member (~19/200 users).
  - Mod detection now parses the real badge list (ADMIN scene) —
    the is_moderator shortcut from v0.6 does not exist in this
    library version and was replaced.
v0.6:
  - BADGE PROBE: logs each unique user's badge-related fields once
    (up to 200 users/session) into badges-sample_<stamp>.txt, so the
    next version can recognize subscribers/superfans/Team members
    from real data. Delivered via ntfy with the other files.
  - Mods are now ALSO detected by TikTok's own moderator flag when
    the library exposes it — a new mod gets greeted even before the
    MODERATORS list is updated. The list still works as before.
  - ntfy hardened: the topic is percent-encoded (a stray non-Latin
    character can no longer kill notifications) and push titles are
    ASCII-only by design.
v0.5.2: reliability — 60s connect timeout kills hung connects,
  5-min stall watchdog kills zombie sessions, and stream-end is
  verified: if the streamer is still live the push says so and recording
  resumes in 5s; empty segments no longer send blank files.
v0.5/v0.5.1: BOT BRAIN (dry-run) — tap-milestone thank-yous and
  moderator greetings, decided in real time, written to a botlog
  ("WOULD POST: ...") instead of posted; greeting triggers on a
  mod's FIRST activity of any kind (TikTok samples join events).
v0.4: ntfy.sh pushes — start ping, live ping, end-of-stream summary
  plus files as attachments. Set NTFY_TOPIC below.
v0.3.1: library parse-error tracebacks silenced (they come from
  message types we don't record, e.g. LinkLayerMessage).

Two CSVs per session (same timestamp) + botlog + badges sample:
  tiktok-chat_<user>_<stamp>.csv    time,username,message
  tiktok-events_<user>_<stamp>.csv  time,username,event,detail
  tiktok-botlog_<user>_<stamp>.txt  bot dry-run decisions
  badges-sample_<user>_<stamp>.txt  raw badge fields per unique user

GIFTS AND MOST EVENT TYPES REQUIRE PYTHON 3.12/3.13 (3.14 breaks
the library's protobuf schemas). On the VM: system Python 3.12.
"""

import asyncio
import csv
import logging
import os
import sys
import time
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone

from TikTokLive import TikTokLiveClient
from TikTokLive.events import (
    ConnectEvent,
    CommentEvent,
    DisconnectEvent,
    LiveEndEvent,
)


def silence_tiktok_logs(client=None) -> None:
    """Silence every logger the library creates, including per-client
    ones that only exist after connection. Harmless message types
    (e.g. WebcastLinkLayerMessage) hit a known schema bug and would
    otherwise flood the console with tracebacks."""
    logging.getLogger("TikTokLive").setLevel(logging.CRITICAL)
    for name in list(logging.Logger.manager.loggerDict):
        if "tiktok" in name.lower():
            logging.getLogger(name).setLevel(logging.CRITICAL)
    if client is not None:
        try:
            client.logger.setLevel(logging.CRITICAL)
        except Exception:
            pass


silence_tiktok_logs()

# --- optional event types: import whatever this library version has ---
OPTIONAL = {}
for _name in [
    "GiftEvent", "EmoteChatEvent", "JoinEvent", "LikeEvent",
    "FollowEvent", "ShareEvent", "SubscribeEvent", "RoomUserSeqEvent",
    "QuestionNewEvent", "EnvelopeEvent", "LivePauseEvent",
    "LiveUnpauseEvent",
]:
    try:
        module = __import__("TikTokLive.events", fromlist=[_name])
        OPTIONAL[_name] = getattr(module, _name)
    except (ImportError, AttributeError):
        OPTIONAL[_name] = None

# ----------------------------- CONFIG ---------------------------------

USERNAME = "target_streamer"   # TikTok handle, without the @
POLL_INTERVAL_SEC = 120        # how often to check "is the streamer live?" when offline
CONNECT_TIMEOUT_SEC = 60       # a connect that hangs longer is abandoned
STALL_TIMEOUT_SEC = 300        # connected but zero events this long -> reconnect
OUTPUT_DIR = "recordings"      # all output files land here
LOG_EVERY_N = 25               # console progress line every N chat rows

# ntfy.sh push notifications — set your topic (the one you subscribed
# to in the ntfy app). Latin letters/digits/dashes only.
NTFY_TOPIC = "CHANGE-ME"
NTFY_SERVER = "https://ntfy.sh"

# ----- Bot brain (Step 1: dry-run — it decides, logs, posts nothing) ---
BOT_ENABLED = True
BOT_DRY_RUN = True             # True = only writes "WOULD POST" lines
MODERATORS = [                 # matched by @handle (or display name).
    "moderator_handle_1",      # Optional: the ADMIN badge is detected
    "moderator_handle_2",      #   automatically, so this list is only a
]                              #   fallback for rooms where badges lag.
VIPS = [                       # important regulars — greeted once per
    "vip_handle_1",            #   session even without any badge;
    "vip_handle_2",            #   matched by @handle or display name
]
LIKE_MILESTONE_STEP = 300      # thank a user at every N taps (300, 600, ...)
BOT_MIN_INTERVAL_SEC = 20      # min seconds between bot messages (anti-spam)
BOT_MAX_QUEUE = 40             # thank-you backlog before drops
BOT_SESSION_CAP = 250          # max bot messages per session;
                               #   greetings are exempt from the cap

# ----- Badge probe (v0.6): learn what the target room's badges look like ------
BADGE_PROBE = True             # one line per unique user, capped below
BADGE_PROBE_MAX_USERS = 200

# ----------------------------------------------------------------------


def now_iso_z() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def _ntfy_url() -> str:
    # percent-encode the topic so a stray non-Latin character can
    # never crash the sender again
    return f"{NTFY_SERVER.rstrip('/')}/{urllib.parse.quote(NTFY_TOPIC)}"


def ntfy_message(title: str, body: str, tags: str = "") -> None:
    """Send a push notification (blocking; called via asyncio.to_thread).
    Titles must stay ASCII; the body may be any UTF-8 text."""
    if not NTFY_TOPIC or NTFY_TOPIC == "CHANGE-ME":
        return
    try:
        req = urllib.request.Request(
            _ntfy_url(), data=body.encode("utf-8"), method="POST")
        req.add_header("Title", title.encode("ascii", "replace").decode())
        if tags:
            req.add_header("Tags", tags)
        urllib.request.urlopen(req, timeout=15).read()
    except Exception as e:
        log(f"ntfy message failed ({type(e).__name__}): {e}")


def ntfy_file(path: str, title: str) -> None:
    """Send a file as an attachment (blocking; called via asyncio.to_thread)."""
    if not NTFY_TOPIC or NTFY_TOPIC == "CHANGE-ME":
        return
    try:
        with open(path, "rb") as fh:
            data = fh.read()
        req = urllib.request.Request(_ntfy_url(), data=data, method="PUT")
        req.add_header("Filename", os.path.basename(path))
        req.add_header("Title", title.encode("ascii", "replace").decode())
        urllib.request.urlopen(req, timeout=60).read()
    except Exception as e:
        log(f"ntfy file send failed ({type(e).__name__}): {e}")


def display_name(user) -> str:
    name = getattr(user, "nickname", None) or getattr(user, "unique_id", None)
    return name or "unknown"


def first_attr(obj, *names, default=None):
    """Return the first present, non-None attribute among names."""
    for n in names:
        v = getattr(obj, n, None)
        if v is not None:
            return v
    return default


def badge_scenes(user) -> set:
    """Set of badge scene names a user carries — from a live probe
    of the target room: ADMIN = moderator, FANS = Team fan-club member,
    USER_GRADE = platform level (noise), FIRST_RECHARGE = activity."""
    scenes = set()
    for attr in ("badge_list", "badges"):
        try:
            for b in getattr(user, attr, None) or []:
                st = getattr(b, "scene_type", None)
                if st is None:
                    continue
                name = getattr(st, "name", None) or str(st)
                scenes.add(str(name).upper())
        except Exception:
            pass
    return scenes


class Session:
    """Two CSVs per live session: chat (v0.4-compatible) + events."""

    def __init__(self) -> None:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        stamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
        self.chat_path = os.path.join(
            OUTPUT_DIR, f"tiktok-chat_{USERNAME}_{stamp}.csv")
        self.events_path = os.path.join(
            OUTPUT_DIR, f"tiktok-events_{USERNAME}_{stamp}.csv")

        self._chat_fh = open(self.chat_path, "w", newline="",
                             encoding="utf-8-sig")
        self._chat = csv.writer(self._chat_fh)
        self._chat.writerow(["time", "username", "message"])
        self._chat_fh.flush()

        self._ev_fh = open(self.events_path, "w", newline="",
                           encoding="utf-8-sig")
        self._ev = csv.writer(self._ev_fh)
        self._ev.writerow(["time", "username", "event", "detail"])
        self._ev_fh.flush()

        self.chat_count = 0
        self.gift_count = 0
        self.event_count = 0
        self.last_write = time.monotonic()

    def write_chat(self, username: str, message: str) -> None:
        self._chat.writerow([now_iso_z(), username, message])
        self._chat_fh.flush()
        self.last_write = time.monotonic()
        self.chat_count += 1
        if self.chat_count % LOG_EVERY_N == 0:
            log(f"  ... {self.chat_count} chat rows "
                f"({self.gift_count} gifts, {self.event_count} events)")

    def write_event(self, username: str, event: str, detail: str = "") -> None:
        self._ev.writerow([now_iso_z(), username, event, detail])
        self._ev_fh.flush()
        self.last_write = time.monotonic()
        self.event_count += 1

    def close(self) -> None:
        for fh in (self._chat_fh, self._ev_fh):
            try:
                fh.close()
            except Exception:
                pass
        log(f"Session closed: {self.chat_count} chat rows "
            f"({self.gift_count} gifts) -> {self.chat_path}")
        log(f"                {self.event_count} events -> "
            f"{self.events_path}")


class BadgeProbe:
    """v0.6: writes each unique user's badge-related fields once, so
    we can learn from real data which fields mark mods, subscribers,
    Team/fan-club members and top gifters in the target room."""

    KEYWORDS = ("badge", "moder", "subscr", "member", "gift",
                "level", "team", "fan", "top")

    def __init__(self, session: "Session") -> None:
        base = session.chat_path.replace("tiktok-chat_", "badges-sample_")
        self.path = base[:-4] + ".txt" if base.endswith(".csv") else base + ".txt"
        self._fh = open(self.path, "w", encoding="utf-8-sig")
        self._fh.write(f"Badge probe — session {datetime.now()}\n"
                       f"one line per unique user (cap "
                       f"{BADGE_PROBE_MAX_USERS}); only non-empty "
                       f"badge-ish fields are listed\n\n")
        self._fh.flush()
        self.seen = set()
        self.count = 0

    def see(self, user) -> None:
        try:
            uid = getattr(user, "unique_id", None) or str(id(user))
            if uid in self.seen or self.count >= BADGE_PROBE_MAX_USERS:
                return
            self.seen.add(uid)
            parts = []
            for a in dir(user):
                if a.startswith("_"):
                    continue
                la = a.lower()
                if not any(k in la for k in self.KEYWORDS):
                    continue
                try:
                    v = getattr(user, a)
                except Exception:
                    continue
                if callable(v):
                    continue
                if v in (None, "", 0, False) or v == []:
                    continue
                r = repr(v)
                if len(r) > 300:
                    r = r[:300] + "..."
                parts.append(f"{a}={r}")
            # first 3 users get a line even with nothing badge-ish,
            # as a baseline of what "no badges" looks like
            if parts or self.count < 3:
                self.count += 1
                name = display_name(user)
                self._fh.write(
                    f"@{uid}  ({name})\n    "
                    + ("\n    ".join(parts) if parts else "(no badge fields)")
                    + "\n\n")
                self._fh.flush()
        except Exception:
            pass

    def close(self) -> None:
        self._fh.write(f"--- {self.count} users sampled ---\n")
        try:
            self._fh.close()
        except Exception:
            pass


class BotBrain:
    """Step-1 bot: watches the event stream, decides what the bot
    WOULD say (tap milestones, mod greetings), rate-limits it, and
    writes every decision to a botlog file. Posts nothing while
    BOT_DRY_RUN is True — Step 2 replaces _deliver() with a real send."""

    def __init__(self, session: "Session") -> None:
        base = session.chat_path.replace("tiktok-chat_", "tiktok-botlog_")
        self.path = base[:-4] + ".txt" if base.endswith(".csv") else base + ".txt"
        self._fh = open(self.path, "w", encoding="utf-8-sig")
        self._fh.write(f"Bot dry-run log — session {datetime.now()}\n"
                       f"milestone step {LIKE_MILESTONE_STEP}, "
                       f"min interval {BOT_MIN_INTERVAL_SEC}s, "
                       f"moderators configured: {len(MODERATORS)}\n\n")
        self._fh.flush()
        self.taps = {}          # user key -> cumulative tap count
        self.milestones = {}    # user key -> last announced milestone
        self.greeted = set()    # mods greeted this session
        self.greet_q: asyncio.Queue = asyncio.Queue()
        self.thank_q: asyncio.Queue = asyncio.Queue()
        self.sent = 0
        self.dropped = 0

    # ---- decisions (called from event handlers) ----

    def on_like(self, name: str, handle: str, count) -> None:
        key = (handle or name).lower()
        try:
            n = max(int(count or 0), 0)
        except (TypeError, ValueError):
            n = 1
        old = self.taps.get(key, 0)
        new = old + n
        self.taps[key] = new
        if LIKE_MILESTONE_STEP <= 0:
            return
        if new // LIKE_MILESTONE_STEP > old // LIKE_MILESTONE_STEP:
            m = (new // LIKE_MILESTONE_STEP) * LIKE_MILESTONE_STEP
            if self.milestones.get(key, 0) >= m:
                return
            self.milestones[key] = m
            if (self.thank_q.qsize() >= BOT_MAX_QUEUE
                    or self.sent >= BOT_SESSION_CAP):
                self.dropped += 1
                return
            self.thank_q.put_nowait(
                (f"Благодаря, {name}, за {new} тапвания! 💗 Ти си топ! 🐺",
                 f"like milestone {m} (session total {new})"))

    def on_presence(self, name: str, handle: str,
                    mod_flag: bool = False,
                    fan_flag: bool = False) -> None:
        """Greet on FIRST sign of presence of any kind (join, like,
        comment, gift, share, follow). Mods = MODERATORS list OR the
        ADMIN badge; superfans = the FANS (Team fan-club) badge.
        One greeting per user per session; mod greeting wins."""
        key = (handle or name).lower()
        if key in self.greeted:
            return
        is_listed = False
        if MODERATORS:
            ids = {s.lower().lstrip("@") for s in (name, handle) if s}
            mods = {m.lower().lstrip("@") for m in MODERATORS}
            is_listed = bool(ids & mods)
        is_vip = False
        if VIPS:
            ids = ids if MODERATORS else {
                s.lower().lstrip("@") for s in (name, handle) if s}
            vips = {v.lower().lstrip("@") for v in VIPS}
            is_vip = bool(ids & vips)
        if is_listed or mod_flag:
            self.greeted.add(key)
            src = "list" if is_listed else "badge"
            self.greet_q.put_nowait(
                (f"Здравей, {name}! 🐺 {name} е модератор — "
                 f"имайте уважение! 🛡️", f"moderator present ({src})"))
        elif is_vip:
            self.greeted.add(key)
            self.greet_q.put_nowait(
                (f"Здравей, {name}! 🐺 Радваме се, че си тук!",
                 "VIP present (list)"))
        elif fan_flag:
            self.greeted.add(key)
            self.greet_q.put_nowait(
                (f"Здравей, {name}! 💎 Верен фен от отбора! 🐺",
                 "team fan present (FANS badge)"))

    # ---- delivery ----

    def _deliver(self, text: str, reason: str, suffix: str = "") -> None:
        tag = "WOULD POST" if BOT_DRY_RUN else "POST"
        line = f"[{now_iso_z()}] {tag}: {text}   ({reason}){suffix}"
        self._fh.write(line + "\n")
        self._fh.flush()
        log(f"BOT {tag}: {text}   ({reason})")
        self.sent += 1
        # Step 2 hook: when BOT_DRY_RUN is False, the real chat send
        # for the bot account will happen here.

    async def worker(self) -> None:
        """Sends at most one message per BOT_MIN_INTERVAL_SEC,
        greetings always ahead of thank-yous."""
        while True:
            if not self.greet_q.empty():
                text, reason = self.greet_q.get_nowait()
            elif not self.thank_q.empty():
                text, reason = self.thank_q.get_nowait()
            else:
                await asyncio.sleep(1)
                continue
            try:
                self._deliver(text, reason)
            except Exception as e:
                log(f"bot deliver error ({type(e).__name__}): {e}")
            await asyncio.sleep(BOT_MIN_INTERVAL_SEC)

    def close(self) -> None:
        for q, kind in ((self.greet_q, "greeting"), (self.thank_q, "thanks")):
            while not q.empty():
                text, reason = q.get_nowait()
                self._deliver(text, reason, "  [still queued at stream end]")
        top = sorted(self.taps.items(), key=lambda kv: -kv[1])[:5]
        self._fh.write("\n--- session summary ---\n")
        self._fh.write(f"bot messages: {self.sent} "
                       f"(dropped over queue cap: {self.dropped})\n")
        self._fh.write("top tappers: " + ", ".join(
            f"{k}={v}" for k, v in top) + "\n")
        try:
            self._fh.close()
        except Exception:
            pass
        log(f"Botlog closed: {self.sent} messages -> {self.path}")


async def record_session(client: TikTokLiveClient) -> None:
    session = Session()
    done = asyncio.Event()
    brain = BotBrain(session) if BOT_ENABLED else None
    brain_task = asyncio.create_task(brain.worker()) if brain else None
    probe = BadgeProbe(session) if BADGE_PROBE else None

    def notice(user) -> None:
        """Common per-user hook: badge probe + mod/superfan check."""
        if probe:
            probe.see(user)
        if brain:
            scenes = badge_scenes(user)
            brain.on_presence(display_name(user),
                              getattr(user, "unique_id", "") or "",
                              mod_flag="ADMIN" in scenes,
                              fan_flag="FANS" in scenes)

    # ---------------- core: connection lifecycle ----------------

    @client.on(ConnectEvent)
    async def on_connect(event):
        silence_tiktok_logs(client)
        active = [n for n, c in OPTIONAL.items() if c]
        missing = [n for n, c in OPTIONAL.items() if not c]
        log(f"Connected to @{USERNAME} (room {client.room_id}). Recording...")
        log("  capturing: chat + "
            + (", ".join(active) if active else "nothing extra"))
        if missing:
            log(f"  not in this library version: {', '.join(missing)}")
        await asyncio.to_thread(
            ntfy_message, f"@{USERNAME} is LIVE",
            f"Recording started (room {client.room_id}).", "red_circle")

    @client.on(LiveEndEvent)
    async def on_live_end(event):
        session.write_event("stream", "live_end")
        log("Stream ended.")
        done.set()

    @client.on(DisconnectEvent)
    async def on_disconnect(event):
        log("Disconnected from the live.")
        done.set()

    # ---------------- chat CSV: comments, emotes, gifts ----------------

    @client.on(CommentEvent)
    async def on_comment(event):
        try:
            text = (event.comment or "").strip() or "[emoji]"
            session.write_chat(display_name(event.user), text)
            notice(event.user)
        except Exception as e:
            log(f"handler error ({type(e).__name__}): {e}")

    if OPTIONAL["EmoteChatEvent"]:
        @client.on(OPTIONAL["EmoteChatEvent"])
        async def on_emote(event):
            try:
                names = []
                for emote in getattr(event, "emotes", None) or []:
                    n = getattr(emote, "name", None) or getattr(
                        getattr(emote, "emote", None), "name", None)
                    names.append(f"[{n}]" if n else "[emoji]")
                session.write_chat(display_name(event.user),
                                   " ".join(names) or "[emoji]")
                notice(event.user)
            except Exception as e:
                log(f"handler error ({type(e).__name__}): {e}")

    if OPTIONAL["GiftEvent"]:
        @client.on(OPTIONAL["GiftEvent"])
        async def on_gift(event):
            try:
                gift = getattr(event, "gift", None)
                streakable = bool(getattr(gift, "streakable", False))
                streaking = bool(getattr(event, "streaking", False))
                if streakable and streaking:
                    return  # mid-streak tick — record only the final total
                count = first_attr(event, "repeat_count", default=1) or 1
                name = getattr(gift, "name", None) or "gift"
                session.gift_count += 1
                session.write_chat(display_name(event.user),
                                   f"[gift] {name} x{count}")
                notice(event.user)
            except Exception as e:
                log(f"handler error ({type(e).__name__}): {e}")

    # ---------------- events CSV: everything else ----------------

    if OPTIONAL["JoinEvent"]:
        @client.on(OPTIONAL["JoinEvent"])
        async def on_join(event):
            try:
                session.write_event(display_name(event.user), "join")
                notice(event.user)
            except Exception as e:
                log(f"handler error ({type(e).__name__}): {e}")

    if OPTIONAL["LikeEvent"]:
        @client.on(OPTIONAL["LikeEvent"])
        async def on_like(event):
            try:
                count = first_attr(event, "count", "like_count", default=1)
                total = first_attr(event, "total", "total_likes", default="")
                detail = (f"+{count}"
                          + (f" (stream total {total})" if total else ""))
                session.write_event(display_name(event.user), "like", detail)
                notice(event.user)
                if brain:
                    brain.on_like(display_name(event.user),
                                  getattr(event.user, "unique_id", "") or "",
                                  count)
            except Exception as e:
                log(f"handler error ({type(e).__name__}): {e}")

    if OPTIONAL["FollowEvent"]:
        @client.on(OPTIONAL["FollowEvent"])
        async def on_follow(event):
            try:
                session.write_event(display_name(event.user), "follow")
                notice(event.user)
            except Exception as e:
                log(f"handler error ({type(e).__name__}): {e}")

    if OPTIONAL["ShareEvent"]:
        @client.on(OPTIONAL["ShareEvent"])
        async def on_share(event):
            try:
                session.write_event(display_name(event.user), "share")
                notice(event.user)
            except Exception as e:
                log(f"handler error ({type(e).__name__}): {e}")

    if OPTIONAL["SubscribeEvent"]:
        @client.on(OPTIONAL["SubscribeEvent"])
        async def on_subscribe(event):
            try:
                session.write_event(display_name(event.user), "subscribe")
                notice(event.user)
            except Exception as e:
                log(f"handler error ({type(e).__name__}): {e}")

    if OPTIONAL["RoomUserSeqEvent"]:
        @client.on(OPTIONAL["RoomUserSeqEvent"])
        async def on_viewers(event):
            try:
                total = first_attr(event, "total", "total_user",
                                   "viewer_count", default="")
                session.write_event("stream", "viewers", str(total))
            except Exception as e:
                log(f"handler error ({type(e).__name__}): {e}")

    if OPTIONAL["QuestionNewEvent"]:
        @client.on(OPTIONAL["QuestionNewEvent"])
        async def on_question(event):
            try:
                q = first_attr(event, "question", default=None)
                text = first_attr(q, "text", "content", default="") if q else ""
                user = getattr(q, "user", None) or getattr(event, "user", None)
                session.write_event(
                    display_name(user) if user else "unknown", "question",
                    str(text))
            except Exception as e:
                log(f"handler error ({type(e).__name__}): {e}")

    if OPTIONAL["EnvelopeEvent"]:
        @client.on(OPTIONAL["EnvelopeEvent"])
        async def on_envelope(event):
            try:
                user = getattr(event, "user", None)
                session.write_event(
                    display_name(user) if user else "unknown", "envelope")
            except Exception as e:
                log(f"handler error ({type(e).__name__}): {e}")

    if OPTIONAL["LivePauseEvent"]:
        @client.on(OPTIONAL["LivePauseEvent"])
        async def on_pause(event):
            session.write_event("stream", "pause")

    if OPTIONAL["LiveUnpauseEvent"]:
        @client.on(OPTIONAL["LiveUnpauseEvent"])
        async def on_unpause(event):
            session.write_event("stream", "unpause")

    # ---------------- run ----------------

    async def watchdog():
        """Kills zombie sessions: connected but no events flowing."""
        while not done.is_set():
            await asyncio.sleep(15)
            if time.monotonic() - session.last_write > STALL_TIMEOUT_SEC:
                log(f"No events for {STALL_TIMEOUT_SEC}s — forcing reconnect.")
                done.set()

    wd_task = asyncio.create_task(watchdog())
    still_live_after = False
    try:
        try:
            await asyncio.wait_for(client.start(),
                                   timeout=CONNECT_TIMEOUT_SEC)
        except asyncio.TimeoutError:
            log(f"Connect hung for {CONNECT_TIMEOUT_SEC}s — abandoning, "
                "will retry from polling.")
            done.set()
        session.last_write = time.monotonic()
        await done.wait()
    finally:
        wd_task.cancel()
        try:
            await client.disconnect()
        except Exception:
            pass
        if brain_task:
            brain_task.cancel()
            try:
                await brain_task
            except (asyncio.CancelledError, Exception):
                pass
        if brain:
            brain.close()
        if probe:
            probe.close()
        session.close()
        # Real end, or did the connection just drop mid-stream?
        try:
            check = TikTokLiveClient(unique_id=f"@{USERNAME}")
            silence_tiktok_logs(check)
            still_live_after = bool(await check.is_live())
        except Exception:
            still_live_after = False
        if still_live_after:
            title = f"@{USERNAME}: connection dropped - resuming"
            tags = "warning"
        else:
            title = f"@{USERNAME} stream ended"
            tags = "checkered_flag"
        summary = (f"{session.chat_count} chat rows, "
                   f"{session.gift_count} gifts, "
                   f"{session.event_count} events"
                   + (f", {brain.sent} bot messages (dry-run)." if brain
                      else "."))
        await asyncio.to_thread(ntfy_message, title, summary, tags)
        if session.chat_count + session.event_count > 0:
            await asyncio.to_thread(
                ntfy_file, session.chat_path, "Chat CSV")
            await asyncio.to_thread(
                ntfy_file, session.events_path, "Events CSV")
            if brain and brain.sent > 0:
                await asyncio.to_thread(
                    ntfy_file, brain.path, "Bot dry-run log")
            if probe and probe.count > 0:
                await asyncio.to_thread(
                    ntfy_file, probe.path, "Badge sample")
    return still_live_after


async def main() -> None:
    py = f"{sys.version_info.major}.{sys.version_info.minor}"
    bot_note = ("bot DRY-RUN on" if BOT_ENABLED and BOT_DRY_RUN
                else "bot LIVE" if BOT_ENABLED else "bot off")
    probe_note = "badge probe on" if BADGE_PROBE else "badge probe off"
    log(f"Recorder v0.8 on Python {py} ({bot_note}, "
        f"{len(MODERATORS)} mods + {len(VIPS)} VIPs configured, "
        f"superfan greetings on, "
        f"{probe_note}). "
        f"Watching @{USERNAME}, polling every {POLL_INTERVAL_SEC}s. "
        f"Ctrl+C to stop.")
    await asyncio.to_thread(
        ntfy_message, "Recorder started",
        f"Watching @{USERNAME}, polling every {POLL_INTERVAL_SEC}s.",
        "rocket")
    if sys.version_info >= (3, 14):
        log("NOTE: Python 3.14 detected — chat records fine, but gifts and "
            "several event types are broken on 3.14. Run with "
            "'py -3.12 recorder.py' for full capture.")
    while True:
        client = TikTokLiveClient(unique_id=f"@{USERNAME}")
        silence_tiktok_logs(client)
        try:
            live = await client.is_live()
        except Exception as e:
            log(f"is_live check failed ({type(e).__name__}: {e}) — "
                f"retrying in {POLL_INTERVAL_SEC}s")
            live = False

        resume_now = False
        if live:
            log("Streamer is LIVE — starting recorder.")
            try:
                resume_now = bool(await record_session(client))
            except Exception as e:
                log(f"Recording error ({type(e).__name__}): {e}")
            if resume_now:
                log("She's still live — resuming in 5s (new segment).")
            else:
                log(f"Back to polling in {POLL_INTERVAL_SEC}s.")

        await asyncio.sleep(5 if resume_now else POLL_INTERVAL_SEC)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped by user.")
