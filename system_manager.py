import mysql.connector as connector
import json
from datetime import datetime


class SystemNotFound(Exception):
    pass


class SystemsManager:
    def __init__(self, config):
        self.config = config
        self.db = config["db"]
        self.red_conf = config["redis"]
        self.systems = []
        self.mapid = config["watcher"]["mapid"]
        self.kills = []

    def fetch_systems(self, mapid):
        con = connector.connect(user=self.db["user"],
                                password=self.db["password"],
                                host=self.db["host"],
                                port=self.db["port"],
                                database=self.db["pathfinder_name"])
        cur = con.cursor()

        cur.execute(
            f"SELECT system.systemId, system.id FROM {self.db['pathfinder_name']}.map, {self.db['pathfinder_name']}.system "
            "WHERE system.active <> 0 AND map.id = system.mapId AND map.id = %s",
            (mapid, ))

        results = [row for row in cur]

        cur.close()
        con.close()

        return results

    def update(self):
        new_systems = self.fetch_systems(self.mapid)
        gone_systems = set(self.systems) - set(new_systems)
        added_systems = set(new_systems) - set(self.systems)
        self.systems = new_systems

        commands = []
        for s in gone_systems:
            commands.append(
                json.dumps({
                    "action": "unsub",
                    "channel": f"system:{s[0]}"
                }))
        for s in added_systems:
            commands.append(
                json.dumps({
                    "action": "sub",
                    "channel": f"system:{s[0]}"
                }))

        return commands

    def remember_kill(self, killid, systemid):
        self.kills.append((killid, systemid))

    def set_rally_point(self, system_id):
        con = connector.connect(user=self.db["user"],
                                password=self.db["password"],
                                host=self.db["host"],
                                port=self.db["port"],
                                database=self.db["pathfinder_name"])
        cur = con.cursor()

        cur.execute(
            f"SELECT count(*) FROM {self.db['pathfinder_name']}.system WHERE system.active <> 0 AND system.systemId = %s",
            (system_id, ))
        if cur.fetchone()[0] == 0:
            cur.close()
            con.close()
            raise SystemNotFound("Could not set RallyPoint")

        timestamp = datetime.utcnow().strftime("%Y-%m-%d %T")

        cur.execute(
            f"UPDATE {self.db['pathfinder_name']}.system SET updated = %s, rallyUpdated = system.updated "
            "WHERE system.systemId = %s", (timestamp, system_id))

        cur.close()
        con.commit()
        con.close()

        if self.config["redis"].get("enabled", True):
            import redis
            r = redis.Redis(host=self.red_conf["host"],
                            port=self.red_conf["port"])
            r.flushall()

    def get_system_of_kill(self, kill_id):
        for k, s in self.kills:
            if k == kill_id:
                return s
        return None
