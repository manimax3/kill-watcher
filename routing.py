from collections import defaultdict, namedtuple
from contextlib import closing
import mysql.connector as connector
import toml
import re


class RoutingManager:

    Connection = namedtuple("Connection", ["source", "target"])

    security_lookup = {
        "L": "LS",
        "H": "HS",
        "0.0": "NS",
    }

    alias_re = re.compile(r"\.?([a-zA-Z]) \S.*")

    def __init__(self, config):
        self.config = config
        self.routing_config = config["routing"]

        self.connections = list()
        self.system_alias = dict()
        self.adj_list = None
        self.predecessors = None
        self.distances = None

    # Returns distance, route
    # if no route found: -1, None
    def shortest_route(self, target, source="ROOT"):
        if source is None or source == "ROOT":
            source = self.routing_config["root"]

        if target not in self.distances:
            return -1, None

        distance = self.distances[target]
        if distance == 0:
            return 0, []

        route = []
        next = target
        while True:
            route.append(next)
            next = self.predecessors[next]

            if next == source:
                break
        return distance, list(reversed(route))

    def fetch_routinginformation(self):
        # Clear out data
        self.connections = list()
        self.system_alias = dict()

        db = self.config["db"]
        dbname = db["pathfinder_name"]
        with closing(
                connector.connect(user=db["user"],
                                  password=db["password"],
                                  host=db["host"],
                                  port=db["port"],
                                  database=dbname)) as conn, closing(
                                      conn.cursor()) as cur:

            # Fetch the connections
            cur.execute(
                f"SELECT s1.systemId,s2.systemId FROM {dbname}.connection,{dbname}.system as s1,{dbname}.system as s2 "
                "WHERE connection.mapId = %s AND s1.id = connection.source AND s2.id = connection.target",
                (self.config["watcher"]["mapid"], ))
            for r in cur:
                self.connections.append(self.Connection(*r))

            # Fetch the systems
            cur.execute(
                "SELECT system.systemId,system.alias FROM system WHERE system.active <> 0 AND system.mapId = %s",
                (self.config["watcher"]["mapid"], ))
            for r in cur:
                self.system_alias[r[0]] = r[1]

        self.make_adjacency_list()
        self.breadth_first_search()

    def breadth_first_search(self):
        root = self.routing_config["root"]
        visited = defaultdict(lambda: False)
        predecessor = defaultdict(lambda: None)
        distance = dict()
        queue = [root]
        distance[root] = 0

        while len(queue) > 0:
            cur = queue.pop(0)
            adj = self.adj_list[cur]
            for a in filter(lambda a: not visited[a], adj):
                if a not in predecessor:
                    predecessor[a] = cur
                    distance[a] = distance[cur] + 1
                queue.append(a)
            visited[cur] = True

        self.predecessors = predecessor
        self.distances = distance

    def make_adjacency_list(self):
        self.adj_list = defaultdict(set)
        for source, target in self.connections:
            self.adj_list[source].add(target)
            self.adj_list[target].add(source)

    def resolve_route_disi_nameing(self, route: list):
        if not route or len(route) == 0:
            return []

        system_name = {}
        system_security = {}

        db = self.config["db"]
        dbname = self.config["db"]["universe_name"]
        with closing(
                connector.connect(user=db["user"],
                                  password=db["password"],
                                  host=db["host"],
                                  port=db["port"],
                                  database=dbname)) as conn, closing(
                                      conn.cursor()) as cur:
            for system in route:
                cur.execute(
                    "SELECT name,security FROM system WHERE system.id = %s",
                    (system, ))
                name, sec = cur.fetchone()
                system_name[system] = name
                system_security[system] = sec

        newnames = []
        for system in route:

            security = self.security_lookup.get(system_security[system],
                                                system_security[system])
            name = system_name[system]
            alias = self.system_alias[system]
            # print(security, name, alias)

            if len(alias) > 0:
                match = self.alias_re.match(alias)
                if match is None:
                    final = f"{security} {alias}"
                else:
                    alias = match.group(1)[0]
                    final = f"{security}.{alias}"
            else:
                final = f"{security}"

            newnames.append(final)
        return newnames


def main():
    config = toml.load("config.toml")
    # print(config["routing"])
    rm = RoutingManager(config)
    rm.fetch_routinginformation()
    # print(rm.distances)
    # print("adj_list", rm.adj_list)
    d, route = rm.shortest_route(30000846)
    route = rm.resolve_route_disi_nameing(route)
    print(" -> ".join(route))


if __name__ == "__main__":
    main()
