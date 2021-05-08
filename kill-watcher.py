import asyncio
import aiohttp
import websockets
import json
import discord
import toml
import logging as log
import re
from datetime import datetime, timezone, timedelta
from collections import defaultdict

from system_manager import SystemsManager, SystemNotFound
import esi

config = toml.load("config.toml")

esi_endpoint = "https://esi.evetech.net/latest"
log.basicConfig(format='%(asctime)s %(message)s',
                level=getattr(log, config["watcher"]["loglevel"], log.INFO))

discord_client = discord.Client()
sm = SystemsManager(config)

re_killurl = re.compile(r"https://zkillboard\.com/kill/([0-9]+)/")
re_sysname = re.compile(r"Kill occurred in (.*)\n")
re_wspace_name = re.compile(r"J\d{6}")


async def consumer(msg):
    msg = json.loads(msg)

    if msg["action"] == "tqStatus":
        log.debug(msg)
    else:
        log.info(msg)

    if not all(k in msg for k in ("killID", "hash")):
        return

    killmail = await esi.fetch_killmail(msg["killID"], msg["hash"])
    system = await esi.fetch_system(killmail["solar_system_id"])

    if system["security_status"] >= 0.5:
        # We dont care about highsec kills
        log.info(f"Killmail {msg['killID']} filtered. Happend in HS")
        return

    if len(config["watcher"]["filter_corporations"]) > 0:
        filter_corporations = set(config["watcher"]["filter_corporations"])

        try:
            defender_corporation = killmail["victim"]["corporation_id"]
            attacker_corporations = set([
                k["corporation_id"] for k in filter(
                    lambda k: "corporation_id" in k, killmail["attackers"])
            ])
        except KeyError:
            defender_corporation = ""
            # Not all of this information is available for npc kills
            attacker_corporations = set()

        if config["watcher"][
                "filter_if_victim"] and defender_corporation in filter_corporations:
            log.info(
                f"Killmail {msg['killID']} filtered. Defender corporation filtered"
            )
            return

        if len(attacker_corporations) > 0 and len(
                filter_corporations.intersection(attacker_corporations)) > 0:
            log.info(
                f"Killmail {msg['killID']} filtered. Attackers Corporation filtered"
            )
            return

    ship_type = killmail["victim"]["ship_type_id"]
    if ship_type in config["watcher"]["filter_ship_types"]:
        log.info(f"Killmail {msg['killID']} filtered. Ship type filtered.")
        return

    kill_time = datetime.fromisoformat(killmail["killmail_time"][:-1] +
                                       "+00:00")
    delta = abs(datetime.now(tz=timezone.utc) - kill_time)
    delta -= timedelta(microseconds=delta.microseconds)

    iswspace = re_wspace_name.match(system["name"])

    # Send data straight to discord
    channel = discord_client.get_channel(config["discord"]["channel_id"])

    if "ping_role_id" in config["discord"] and \
            (not config["discord"]["ping_only_wspace"] or iswspace is not None):
        ping_role = f"<@&{config['discord']['ping_role_id']}> "
    else:
        ping_role = ""

    attacker_count = len(killmail["attackers"])
    corp_count = defaultdict(lambda: 0)
    for attacker_corp in map(
            lambda a: a["corporation_id"],
            filter(lambda a: "corporation_id" in a, killmail["attackers"])):
        corp_count[attacker_corp] += 1

    if len(corp_count) > 0:
        main_corp = max(corp_count.items(), key=lambda i: i[1])[0]
    else:
        main_corp = None

    sm.remember_kill(msg["killID"], killmail["solar_system_id"])

    final_message = [
        ping_role, f"Kill occurred in {system['name']}",
        f"Happend {delta} ago.", f"Attackers: {attacker_count}"
    ]

    if main_corp is not None:
        corp_info, alli_info = await esi.fetch_corporation(main_corp,
                                                           alliance=True)
        alli_name = ' (' + alli_info["name"] + ')' if alli_info else ""
        final_message.append(f"Attacking Corp: {corp_info['name']}{alli_name}")

    final_message.append(msg["url"])

    final_message = '\n'.join(final_message)
    msg = await channel.send(final_message)
    await msg.add_reaction(config["discord"]["react_emoji_id"])


async def consumer_handler(websocket):
    while True:
        async for msg in websocket:
            await consumer(msg)
        await asyncio.sleep(0.5)


async def producer_handler(websocket):
    await websocket.send(json.dumps({"action": "sub", "channel": "public"}))
    while True:
        commands = sm.update()
        for cmd in commands:
            await websocket.send(cmd)
        await asyncio.sleep(config["watcher"]["refresh"])


async def connect():
    async with websockets.connect("wss://zkillboard.com/websocket/") as ws:
        d = asyncio.create_task(
            discord_client.start(config["discord"]["bot_token"]))
        c = asyncio.create_task(consumer_handler(ws))
        p = asyncio.create_task(producer_handler(ws))
        await asyncio.gather(p, c, d)


@discord_client.event
async def on_ready():
    log.info(f"Initialized {discord_client.user.name}")


@discord_client.event
async def on_raw_reaction_add(payload):
    channel = discord_client.get_channel(config["discord"]["channel_id"])

    if payload.channel_id != channel.id or payload.user_id == discord_client.user.id \
            or payload.emoji.name != config["discord"]["react_emoji_id"]:
        return

    msg = await channel.fetch_message(payload.message_id)

    if msg.author != discord_client.user:
        return

    match = re_killurl.search(msg.content)

    if match is None:
        log.info("Reaction did not contain a zkillboard link. Skipping")
        return

    killid = int(match.groups(1)[0])
    system_id = sm.get_system_of_kill(killid)

    if system_id is None:
        log.info(
            f"Could not lookup systemid for kill {killid}. Asking zkillboard")
        killmail = await esi.fetch_killmail(killid, hash=None)
        system_id = killmail["solar_system_id"]
        if system_id is not None and killid is not None:
            sm.remember_kill(killid, system_id)

    match = re_sysname.search(msg.content)
    if match is None:
        return

    sysname = match.groups(1)[0]

    try:
        sm.set_rally_point(system_id)
        await channel.send(f"Rally point set to {sysname}.")
    except SystemNotFound:
        await channel.send(
            f"Could not set rally point. System {sysname} no longer on the map."
        )


asyncio.get_event_loop().run_until_complete(connect())
