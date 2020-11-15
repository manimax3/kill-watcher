import asyncio
import aiohttp
import websockets
import json
import discord
import toml
import requests
import mysql.connector as connector

class SystemsManager:
    def __init__(self, mapname):
        self.systems = []
        self.mapname = mapname

    def update(self):
        new_systems = fetch_systems(self.mapname)
        gone_systems = set(self.systems) - set(new_systems)
        added_systems = set(new_systems) - set(self.systems)
        self.systems = new_systems

        commands = []
        for s in gone_systems:
            commands.append(json.dumps({"action": "unsub", "channel": f"system:{s[0]}"}))
        for s in added_systems:
            commands.append(json.dumps({"action": "sub", "channel": f"system:{s[0]}"}))

        return commands


config = toml.load("config.toml")
db = config["db"]
connection_working = False

esi_endpoint = "https://esi.evetech.net/latest"

discord_client = discord.Client()

def fetch_systems(mapname):
    con = connector.connect(user=db["user"], password=db["password"],
                            host=db["host"], port=db["port"], database=db["pathfinder_name"])
    cur = con.cursor()

    cur.execute(f"SELECT system.systemId FROM {db['pathfinder_name']}.map, {db['pathfinder_name']}.system "
                "WHERE map.id = system.mapId AND map.name = %s", (mapname,))

    results = [row for row in cur]

    cur.close()
    con.close()

    return results

async def consumer(msg):
    connection_working = True
    msg = json.loads(msg)
    print(msg)
    if "littlekill" != msg["action"].lower():
        return

    async with aiohttp.ClientSession() as s:
        async with s.get(
            f"{esi_endpoint}/killmails/{msg['killID']}/{msg['hash']}/?datasource=tranquility") \
                as response:
            killmail = await response.json()
        async with s.get(
            f"{esi_endpoint}/systems/{killmail['solar_system_id']}/?datasource=tranquility") \
                as response:
            system = await response.json()

    if system["security_status"] >= 0.5:
        # We dont care about highsec kills
        return

    # Send data straight to discord
    channel = discord_client.get_channel(config["discord"]["channel_id"])
    await channel.send(f"Kill occured in {system['name']}\n{msg['url']}")

async def consumer_handler(websocket):
    while True:
        async for msg in websocket:
            await consumer(msg)
        await asyncio.sleep(0.5)

async def producer_handler(websocket):
    sm = SystemsManager(config["watcher"]["map"])
    await websocket.send(json.dumps({"action": "sub", "channel": "public"}))
    while True:
        commands = sm.update()
        for cmd in commands:
            await websocket.send(cmd)
        await asyncio.sleep(config["watcher"]["refresh"])

async def connect():
    async with websockets.connect("wss://zkillboard.com/websocket/") as ws:
        d = asyncio.create_task(discord_client.start(config["discord"]["bot_token"]))
        c = asyncio.create_task(consumer_handler(ws))
        p = asyncio.create_task(producer_handler(ws))
        await asyncio.gather(p, c, d)

@discord_client.event
async def on_ready():
    print(f"Intiaialized {discord_client.user.name}")

asyncio.get_event_loop().run_until_complete(connect())
