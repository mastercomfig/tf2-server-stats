import asyncio
import datetime
import ipaddress
import math
import os
import random
import sys
import time
import traceback
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path

import a2s
import aiohttp
import orjson
import tinydb
from dotenv import load_dotenv

load_dotenv(override=True)

STEAM_API_KEY = os.getenv("STEAM_API_KEY")
if not STEAM_API_KEY:
    print("Need to pass in STEAM_API_KEY")
    sys.exit(1)
COMFIG_API_URL = os.getenv("COMFIG_API_URL")
if not COMFIG_API_URL:
    print("Need to pass in COMFIG_API_URL")
    sys.exit(1)
COMFIG_API_KEY = os.getenv("COMFIG_API_KEY")
if not COMFIG_API_KEY:
    print("Need to pass in COMFIG_API_KEY")
    sys.exit(1)
STEAM_API_PARAM = {"key": STEAM_API_KEY, "format": "json"}
QUERY_INTERVAL = 0.833 * 60
QUERY_INTERVAL_VARIANCE = 3.333 * 60
QUERY_FILTER = r"\appid\440\gamedir\tf\empty\1"
QUERY_LIMIT = "20000"

DEBUG = os.getenv("QUICKPLAY_DEBUG") is not None
DEBUG_SKIP_SERVERS = os.getenv("QUICKPLAY_DEBUG_SKIP_SERVERS") is not None

OVERVIEW_INTERVAL = 300

APP_ID = 440
APP_NAME = "tf"
APP_FULL_NAME = "Team Fortress"

DB = tinydb.TinyDB(Path("./db_servers.json"))
ban_table = DB.table("bans")

TIMESTAMP_TIMEZONE = datetime.timezone.utc


def utcnow() -> datetime.datetime:
    return datetime.datetime.now(tz=TIMESTAMP_TIMEZONE)


EMPTY_DICT = {}

TableValueType = int | float | str | bool
TableContainerValueType = "TableValueType | TableContainerType"
TableContainerType = list[TableContainerValueType] | dict[str, TableContainerValueType]
TableType = TableValueType | TableContainerType

Store = tinydb.Query()


def get_value(key: str, *, default: TableType = None, table) -> TableType:
    """
    Gets from the key value DB table.
    """
    res = table.get(Store.k == key)
    if res:
        return res["v"]
    if default is not None:
        set_value(key, default, table=table)
    return default


def set_value(key: str, val: TableType, *, table):
    """
    Sets to the key value DB table.
    """
    table.upsert({"k": key, "v": val}, Store.k == key)


def del_value(key: str, *, table):
    """
    Deletes from the key value DB table.
    """
    table.remove(Store.k == key)


def update_value(
    update_fn: Callable[[TableType], TableType],
    key: str,
    *,
    default: TableType = None,
    table,
) -> TableType:
    """
    Gets an existing value in the key value DB table and updates it using update_fn.
    :return: The new value
    """
    old = get_value(key, default=default, table=table)
    new = update_fn(old)
    set_value(key, new, table=table)
    return new


def chaos(max=300) -> float:
    if max == 0:
        return 0.0
    return random.uniform(0, max)


def shuffle(val, pct=0.1) -> float:
    return val + random.normalvariate(0, math.sqrt(val * pct))


all_player_names: dict[str, float] = {}
player_names: dict[str, float] = {}
player_counts: dict[str, int] = defaultdict(int)

map_players: dict[str, set[str]] = defaultdict(set)
player_maps: dict[str, set[str]] = defaultdict(set)

server_players: dict[str, set[str]] = defaultdict(set)
player_servers: dict[str, set[str]] = defaultdict(set)

server_capacities: dict[str, int] = defaultdict(int)
tags: dict[str, int] = defaultdict(int)


next_overview_resp_time = 0
last_server_version = 0


async def get_server_version(api_session: aiohttp.ClientSession) -> int:
    global next_overview_resp_time
    global last_server_version
    current_time = time.monotonic()
    if last_server_version and current_time < next_overview_resp_time:
        return last_server_version
    try:
        async with api_session.get(
            "/IGCVersion_440/GetServerVersion/v1/", params=STEAM_API_PARAM
        ) as resp:
            body = await resp.read()
            body = orjson.loads(body)
            server_version = body.get("result", EMPTY_DICT).get("min_allowed_version")
            if server_version:
                next_overview_resp_time = current_time + OVERVIEW_INTERVAL + chaos()
                last_server_version = server_version
    except Exception:
        traceback.print_exc()
    return last_server_version


def by_value(x):
    return x[1]


async def query_runner(
    api_session: aiohttp.ClientSession, comfig_session: aiohttp.ClientSession
):
    server_params = {
        "key": STEAM_API_KEY,
        "format": "json",
        "limit": QUERY_LIMIT,
        "filter": QUERY_FILTER,
    }
    banned_ips = set(get_value("ips", default=[], table=ban_table))
    banned_ids = set(get_value("ids", default=[], table=ban_table))
    pending_servers = []
    query_intervals = []
    while True:
        if len(query_intervals) == 0:
            query_1 = QUERY_INTERVAL + chaos(QUERY_INTERVAL_VARIANCE)
            query_2 = QUERY_INTERVAL + chaos(QUERY_INTERVAL_VARIANCE)
            query_3 = 10 * 60 - (query_1 + query_2)
            query_intervals = [query_3, query_2, query_1]
        next_query_interval = query_intervals.pop()
        server_version = await get_server_version(api_session)
        try:
            if server_version:
                try:
                    async with api_session.get(
                        "/IGameServersService/GetServerList/v1/",
                        params=server_params,
                    ) as resp:
                        body = await resp.read()
                        body = body.decode("utf-8", errors="replace")
                        body = orjson.loads(body)
                        pending_servers = body["response"]["servers"]
                except:
                    traceback.print_exc()

                now = utcnow().timestamp()
                current_counts = defaultdict(int)

                async def calc_server(server):
                    count_players = True
                    # not tf, leave
                    if server["appid"] != APP_ID:
                        return 0
                    if server["gamedir"] != APP_NAME:
                        return 0
                    if server["product"] != APP_NAME:
                        return 0
                    num_players = server["players"]
                    if num_players < 2:
                        count_players = False
                    max_players = server["max_players"]
                    if max_players < 6:
                        count_players = False
                    # check if out of date
                    if int(server["version"]) < server_version:
                        count_players = False
                    # check for map
                    map = server.get("map")
                    if not map:
                        count_players = False
                    # check for ban
                    steamid = server["steamid"]
                    if steamid in banned_ids:
                        return 0
                    addr = server["addr"]
                    ip, port = addr.split(":")
                    if ip in banned_ips:
                        return 0
                    bots = server["bots"]
                    players = []
                    name = (
                        server["name"]
                        .replace("\u0001", "")
                        .replace("\t", "")
                        .encode("raw_unicode_escape")
                        .decode("unicode_escape")
                        .strip()
                    )
                    gametypes = server.get("gametype", "").lower().split(",")
                    for gametype in gametypes:
                        tags[gametype] += 1
                    if addr.startswith("169.254"):
                        ip_address = ipaddress.ip_address(ip)
                        fake_ip = int(ip_address)
                        async with api_session.get(
                            "/IGameServersService/QueryByFakeIP/v1/",
                            params={
                                "key": STEAM_API_KEY,
                                "fake_ip": fake_ip,
                                "fake_port": port,
                                "app_id": APP_ID,
                                "query_type": 2,
                            },
                        ) as resp:
                            body = await resp.read()
                            body = body.decode("utf-8", errors="replace")
                            body = orjson.loads(body)
                            players_query = body["response"]["players_data"].get(
                                "players", []
                            )
                            players = [player["name"] for player in players_query]
                    else:
                        try:
                            players_query = await a2s.aplayers((ip, port))
                            players = [player.name for player in players_query]
                        except OSError:
                            print("ERROR IN A2S QUERY FOR", addr)
                            return 0
                        except:
                            return 0
                    for player in players:
                        if (
                            len(player) > 3
                            and player[0] == "("
                            and (player[2] == ")" or player[3] == ")")
                        ):
                            count_ahead = 3 if player[2] == ")" else 4
                            player = player[count_ahead:]
                        if count_players or player in player_names:
                            player_names[player] = now
                        all_player_names[player] = now
                        current_counts[player] += 1
                        map_players[map].add(player)
                        player_maps[player].add(map)
                        server_players[name].add(player)
                        player_servers[player].add(name)

                    server_capacities[str(max_players)] += 1

                    return num_players

                server_infos = await asyncio.gather(
                    *[calc_server(server) for server in pending_servers]
                )
                for player, count in current_counts.items():
                    player_counts[player] = max(player_counts[player], count)
                players = sum(server_infos)
                print("Concurrent Players:", players)

                try:
                    async with api_session.get(
                        "/ISteamUserStats/GetNumberOfCurrentPlayers/v1/",
                        params={"appid": APP_ID},
                    ) as resp:
                        body = await resp.read()
                        body = orjson.loads(body)
                        print("Online Players:", body["response"]["player_count"])
                except:
                    traceback.print_exc()

                print("Unique Players:", len(player_names))

                with open("all_players.json", "wb") as fp:
                    players = []
                    player_list = list(all_player_names.keys())
                    player_list.sort()
                    for player in player_list:
                        last_seen = all_player_names[player]
                        max_count = player_counts[player]
                        maps = list(player_maps[player])
                        servers = list(player_servers[player])
                        players.append(
                            {
                                "name": player,
                                "count": max_count,
                                "seen": last_seen,
                                "maps": maps,
                                "servers": servers,
                            }
                        )
                    fp.write(orjson.dumps(players, option=orjson.OPT_INDENT_2))

                with open("players.json", "wb") as fp:
                    players = []
                    player_list = list(player_names.keys())
                    player_list.sort()
                    for player in player_list:
                        last_seen = player_names[player]
                        max_count = player_counts[player]
                        maps = list(player_maps[player])
                        servers = list(player_servers[player])
                        players.append(
                            {
                                "name": player,
                                "count": max_count,
                                "seen": last_seen,
                                "maps": maps,
                                "servers": servers,
                            }
                        )
                    fp.write(orjson.dumps(players, option=orjson.OPT_INDENT_2))
                with open("server_stats.json", "wb") as fp:
                    s2p = {}
                    for server, players in server_players.items():
                        s2p[server] = len(players)
                    s2p = dict(sorted(s2p.items(), key=by_value))
                    my_tags = dict(sorted(tags.items(), key=by_value))
                    my_caps = dict(sorted(server_capacities.items(), key=by_value))
                    m2p = {}
                    for map, players in map_players.items():
                        m2p[map] = len(players)
                    m2p = dict(sorted(m2p.items(), key=by_value))
                    stats = {
                        "tags": my_tags,
                        "caps": my_caps,
                        "players": s2p,
                        "maps": m2p,
                    }
                    fp.write(orjson.dumps(stats, option=orjson.OPT_INDENT_2))

        except Exception:
            traceback.print_exc()

        print("Sleeping...")
        await asyncio.sleep(next_query_interval)
        print("Continuing...")


def encode_json(obj):
    orjson.dumps(obj).decode("utf-8")


async def main():
    async with aiohttp.ClientSession(
        base_url="https://api.steampowered.com", raise_for_status=True
    ) as api_session:
        async with aiohttp.ClientSession(
            base_url=COMFIG_API_URL, json_serialize=encode_json
        ) as comfig_session:
            await query_runner(api_session, comfig_session)


def start():
    asyncio.run(main())
