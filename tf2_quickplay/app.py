import asyncio
import datetime
import json
import math
import os
import random
import sys
import tarfile
import time
import traceback
import urllib.request
from collections.abc import Callable
from pathlib import Path

import a2s
import aiohttp
import geoip2
import geoip2.database
import geopy.distance
import tinydb
import ujson
import vdf

STEAM_API_KEY = os.getenv("STEAM_API_KEY")
if not STEAM_API_KEY:
    print("Need to pass in STEAM_API_KEY")
    sys.exit(1)
COMFIG_API_KEY = os.getenv("COMFIG_API_KEY")
if not COMFIG_API_KEY:
    print("Need to pass in COMFIG_API_KEY")
    sys.exit(1)
GEOIP_KEY = os.getenv("GEOIP_KEY")
if not GEOIP_KEY:
    print("Need to pass in GEOIP_KEY")
    sys.exit(1)
STEAM_API_PARAM = {"key": STEAM_API_KEY}
QUERY_INTERVAL = 60
QUERY_FILTER = r"\appid\440\gamedir\tf\secure\1\dedicated\1\full\1\ngametype\hidden,friendlyfire,highlander,noquickplay,trade,dmgspread,mvm,pve\steamblocking\1\nor\1\white\1"
QUERY_LIMIT = "20000"

CONTINENTS = {
    0: set(["NA"]),
    1: set(["NA"]),
    2: set(["SA"]),
    3: set(["EU"]),
    4: set(["AS"]),
    5: set(["OC"]),
    6: set(["AS", "EU"]),
    7: set(["AF"]),
}

DEBUG = os.getenv("QUICKPLAY_DEBUG") is not None

OVERVIEW_INTERVAL = 300

CDN_BASE_URL = "https://media.steampowered.com"

APP_ID = 440
APP_NAME = "tf"
APP_FULL_NAME = "Team Fortress"

MIN_PLAYER_CAP = 18
MAX_PLAYER_CAP = 101
FULL_PLAYERS = 33

SERVER_HEADROOM = 1

DB = tinydb.TinyDB(Path("./db.json"))
rep_table = DB.table("rep")
ban_table = DB.table("bans")

HOLIDAYS = {"christmas": 12, "halloween": 10}

GAMEMODE_TO_TAG = {
    "attack_defense": "cp",
    "ctf": "ctf",
    "capture_point": "cp",
    "koth": "cp",
    "payload": "payload",
    "payload_race": "payload",
    "arena": "arena",
    "special_events": "",
    "alternative": "",
}

ANY_VALID_TAGS = set(
    ["cp", "ctf", "sd", "payload", "rd", "pd", "tc", "powerup", "passtime", "misc"]
)

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
    return random.uniform(0, max)


def shuffle(val, pct=0.1) -> float:
    return val + random.normalvariate(0, math.sqrt(val * pct))


last_overview_resp = None
next_overview_resp_time = 0
last_items_game_resp = None
last_server_version = 0


async def req_items_game(
    api_session: aiohttp.ClientSession, cdn_session: aiohttp.ClientSession
) -> tuple[dict, bool, int]:
    global last_overview_resp
    global next_overview_resp_time
    global last_items_game_resp
    global last_server_version
    updated = False
    current_time = time.monotonic()
    if last_items_game_resp is not None and current_time < next_overview_resp_time:
        return last_items_game_resp, updated, last_server_version
    try:
        async with api_session.get(
            "/IEconItems_440/GetSchemaOverview/v1/", params=STEAM_API_PARAM
        ) as resp:
            body = await resp.json(encoding="latin-1")
            new_overview_resp: str | None = body.get("result", EMPTY_DICT).get(
                "items_game_url"
            )
            if new_overview_resp:
                new_overview_resp = new_overview_resp.replace(
                    "http://media.steampowered.com", ""
                )
                next_overview_resp_time = current_time + (
                    (OVERVIEW_INTERVAL + chaos()) * 1000
                )
                if new_overview_resp != last_overview_resp:
                    last_overview_resp = new_overview_resp
                    async with cdn_session.get(last_overview_resp) as items_game_resp:
                        items_game_body = await items_game_resp.text(encoding="latin-1")
                        updated = True
                        last_items_game_resp = vdf.loads(
                            items_game_body, mapper=vdf.VDFDict
                        )["items_game"]
        async with api_session.get(
            "/IGCVersion_440/GetServerVersion/v1/", params=STEAM_API_PARAM
        ) as resp:
            body = await resp.json(encoding="latin-1")
            server_version = body.get("result", EMPTY_DICT).get("min_allowed_version")
            if server_version:
                last_server_version = server_version
    except Exception:
        traceback.print_exc()
    return last_items_game_resp, updated, last_server_version


def lerp(in_a, in_b, out_a, out_b, x):
    return out_a + ((out_b - out_a) * (x - in_a)) / (in_b - in_a)


def get_score(server):
    return server["score"]


def score_server(humans: int, bots: int, max_players: int) -> float:
    new_humans = humans + 1
    new_total_players = new_humans + bots

    if max_players > FULL_PLAYERS:
        max_players = FULL_PLAYERS

    if new_total_players + SERVER_HEADROOM > max_players:
        return -100.0

    if new_humans == 1:
        return -0.3

    count_low = max_players // 3
    count_ideal = (max_players * 5) // 6

    score_low = 0.1
    score_ideal = 1.5
    score_fuller = 0.5

    if new_humans <= count_low:
        return lerp(0, count_low, 0.0, score_low, new_humans)
    elif new_humans <= count_ideal:
        return lerp(count_low, count_ideal, score_low, score_ideal, new_humans)
    else:
        return lerp(count_ideal, max_players, score_ideal, score_fuller, new_humans)


async def query_runner(
    geoasn: geoip2.database.Reader,
    geoip: geoip2.database.Reader,
    api_session: aiohttp.ClientSession,
    cdn_session: aiohttp.ClientSession,
    comfig_session: aiohttp.ClientSession,
):
    server_params = {"key": STEAM_API_KEY, "limit": QUERY_LIMIT, "filter": QUERY_FILTER}
    gamemodes: dict[str, set[str]] = {}
    map_gamemode: dict[str, str] = {}
    banned_ips = set(get_value("ips", default=[], table=ban_table))
    banned_ids = set(get_value("ids", default=[], table=ban_table))
    banned_name_search = get_value("names", default=[], table=ban_table)
    banned_tags = set(get_value("tags", default=[], table=ban_table))
    my_ip = "127.0.0.1"
    async with aiohttp.ClientSession("https://api.ipify.org") as ip_session:
        async with ip_session.get("/?format=json") as resp:
            body = await resp.json()
            my_ip = body["ip"]
    my_city = geoip.city(my_ip)
    my_lon = my_city.location.longitude
    my_lat = my_city.location.latitude
    my_point = (my_lat, my_lon)
    while True:
        items_game, updated, server_version = await req_items_game(
            api_session, cdn_session
        )
        try:
            if items_game:
                if updated:
                    map_gamemode = {}
                    gamemodes = {}
                    matchmaking = items_game["matchmaking_categories"]
                    valid_types = set()
                    for category, details in matchmaking.items():
                        match_groups = details["valid_match_groups"]
                        for match_group, value in match_groups.items():
                            if (
                                match_group == "MatchGroup_Casual_12v12"
                                and value == "1"
                            ):
                                valid_types.add(category)
                                break
                    maps = items_game["maps"]
                    for gamemode, details in maps.items():
                        mm_type = details["mm_type"]
                        if mm_type not in valid_types:
                            continue
                        restrictions = details.get("restrictions")
                        if restrictions:
                            now = utcnow()
                            month = now.month
                            passed = True
                            for restriction, name in restrictions.items():
                                if restriction == "holiday":
                                    holiday_month = HOLIDAYS.get(name)
                                    if month != holiday_month:
                                        passed = False
                                else:
                                    passed = False
                                    break
                            if not passed:
                                continue
                        maplist = details["maplist"]
                        gamemode_maps = set()
                        if (
                            mm_type == "special_events" or mm_type == "alternative"
                        ) and gamemode != "payload_race":
                            gamemode = mm_type
                        for map_info in maplist.values():
                            name = map_info["name"]
                            enabled = map_info["enabled"] == "1"
                            if enabled:
                                gamemode_maps.add(name)
                                map_gamemode[name] = gamemode
                        if (
                            mm_type == "special_events" or mm_type == "alternative"
                        ) and gamemode != "payload_race":
                            if mm_type in gamemodes:
                                gamemodes[mm_type].update(gamemode_maps)
                            else:
                                gamemodes[mm_type] = gamemode_maps
                        else:
                            gamemodes[gamemode] = gamemode_maps
                    async with comfig_session.post(
                        "/api/schema/update",
                        headers={"Authorization": f"Bearer {COMFIG_API_KEY}"},
                        json={
                            "schema": {
                                "map_gamemodes": map_gamemode,
                                "gamemodes": {k: list(v) for k, v in gamemodes.items()},
                            }
                        },
                    ) as api_resp:
                        print(await api_resp.text())
                async with api_session.get(
                    "/IGameServersService/GetServerList/v1/",
                    params=server_params,
                ) as resp:
                    body = await resp.json(encoding="latin-1")
                    pending_servers = body["response"]["servers"]

                    async def calc_server(server):
                        addr = server["addr"]
                        # skip servers with SDR
                        if addr.startswith("169.254"):
                            if DEBUG:
                                return {
                                    "score": -999,
                                    "removal": "sdr",
                                    "name": server["name"],
                                }
                            else:
                                return None
                        # check for steam ID
                        steamid = server["steamid"]
                        if not steamid:
                            if DEBUG:
                                return {"score": -999, "removal": "nosteam"}
                            else:
                                return None
                        # not tf, leave
                        if server["appid"] != APP_ID:
                            if DEBUG:
                                return {"score": -999, "removal": "noappid"}
                            else:
                                return None
                        if server["gamedir"] != APP_NAME:
                            if DEBUG:
                                return {"score": -999, "removal": "nogamedir"}
                            else:
                                return None
                        if server["product"] != APP_NAME:
                            if DEBUG:
                                return {"score": -999, "removal": "noprod"}
                            else:
                                return None
                        # check max players
                        max_players = server["max_players"]
                        if max_players < MIN_PLAYER_CAP:
                            # not enough max_players
                            if DEBUG:
                                return {
                                    "score": -999,
                                    "removal": "<18",
                                    "map": server["map"],
                                    "players": server["players"],
                                    "name": server["name"],
                                }
                            else:
                                return None
                        if max_players > MAX_PLAYER_CAP:
                            # too much max_players
                            if DEBUG:
                                return {"score": -999, "removal": ">101"}
                            else:
                                return None
                        num_players = server["players"]
                        if num_players >= max_players:
                            # lying about players
                            if DEBUG:
                                return {"score": -999, "removal": "playercaplie"}
                            else:
                                return None
                        # check if out of date
                        if int(server["version"]) < server_version:
                            if DEBUG and False:
                                return {"score": -999, "removal": "outofdate"}
                            else:
                                return None
                        # check if it's a casual map
                        map = server["map"]
                        if map not in map_gamemode:
                            if DEBUG and False:
                                return {"score": -999, "removal": "badmap", "map": map}
                            else:
                                return None
                        # check for ban
                        if server["steamid"] in banned_ids:
                            if DEBUG:
                                return {"score": -999, "removal": "steamban"}
                            else:
                                return None
                        ip, port = addr.split(":")
                        if ip in banned_ips:
                            if DEBUG:
                                return {"score": -999, "removal": "ipban"}
                            else:
                                return None
                        # check for gametype
                        gametype = set(server["gametype"].lower().split(","))
                        # is lying about max players?
                        if max_players > 24 and "increased_maxplayers" not in gametype:
                            if DEBUG:
                                return {
                                    "score": -999,
                                    "removal": "-maxplayers",
                                    "map": server["map"],
                                    "players": server["players"],
                                    "name": server["name"],
                                }
                            else:
                                return None
                        if max_players <= 24 and "increased_maxplayers" in gametype:
                            if DEBUG:
                                return {
                                    "score": -999,
                                    "removal": "+maxplayers",
                                    "map": server["map"],
                                    "players": server["players"],
                                    "name": server["name"],
                                }
                            else:
                                return None
                        # is it any of the gamemodes we want?
                        found_valid_gametype = (
                            len(gametype.intersection(ANY_VALID_TAGS)) > 0
                        )
                        if not found_valid_gametype:
                            if DEBUG:
                                return {
                                    "score": -999,
                                    "removal": "nogametype",
                                    "gametype": list(gametype),
                                    "name": server["name"],
                                    "map": server["map"],
                                    "players": server["players"],
                                }
                            else:
                                return None
                        # is it the gamemode we want?
                        expected_gamemode = GAMEMODE_TO_TAG.get(map_gamemode[map])
                        if expected_gamemode and expected_gamemode not in gametype:
                            if DEBUG:
                                return {
                                    "score": -999,
                                    "removal": "unexpectedtag",
                                    "map": map,
                                    "expected": expected_gamemode,
                                    "gametype": list(gametype),
                                    "players": server["players"],
                                    "name": server["name"],
                                }
                            else:
                                return None
                        # check for tag errors
                        found_valid_gametype = (
                            len(gametype.intersection(banned_tags)) < 1
                        )
                        if not found_valid_gametype:
                            if DEBUG:
                                return {
                                    "score": -999,
                                    "removal": "badgametype",
                                    "gametype": list(gametype),
                                    "map": server["map"],
                                    "players": server["players"],
                                    "name": server["name"],
                                }
                            else:
                                return None
                        # check for name errors
                        name = server["name"]
                        lower_name = name.lower()
                        bad_name = False
                        for invalid in banned_name_search:
                            if invalid in lower_name:
                                bad_name = True
                        if bad_name:
                            if DEBUG:
                                return {
                                    "score": -999,
                                    "removal": "badname",
                                    "map": server["map"],
                                    "players": server["players"],
                                    "name": server["name"],
                                }
                            else:
                                return None
                        bots = server["bots"]
                        rep = get_value(steamid, table=rep_table)
                        if rep is None:
                            rep = 0
                        score = rep + 6
                        score += score_server(num_players, bots, max_players)
                        if score < 0.1:
                            if DEBUG:
                                return {
                                    "score": -999,
                                    "removal": "lowscore",
                                    "score": score,
                                    "players": num_players,
                                    "bots": bots,
                                    "max": max_players,
                                }
                            else:
                                return None
                        try:
                            server_query = await a2s.ainfo((ip, port))
                        except:
                            if DEBUG:
                                return {
                                    "score": 0,
                                    "removal": "timeout",
                                    "map": server["map"],
                                    "players": server["players"],
                                    "name": server["name"],
                                }
                            else:
                                return None
                        if server_query.password_protected:
                            if DEBUG:
                                return {"score": 0, "removal": "pass"}
                            else:
                                return None
                        if server_query.game != APP_FULL_NAME:
                            if DEBUG:
                                return {
                                    "score": -999,
                                    "removal": "incorrectgame",
                                    "name": name,
                                    "game": server_query.game,
                                    "players": server["players"],
                                }
                            else:
                                return None
                        # shift the scores around a little bit so we get some variance in sorting
                        if score == 6.025:
                            score = shuffle(score, pct=0.008)
                        # calculate ping score
                        ping = server_query.ping * 1000
                        city = geoip.city(ip)
                        country = city.country.iso_code
                        continent = city.continent.code
                        server_region = server["region"]
                        # TODO: do something with non-matching regions
                        lon = city.location.longitude
                        lat = city.location.latitude
                        point = (lat, lon)
                        # asn = geoasn.asn(ip)
                        # aso = asn.autonomous_system_organization
                        # TODO: do something with malicious ASN usage for fake pings
                        dist = geopy.distance.distance(my_point, point).km
                        ideal = dist / 60
                        overhead = max(ping - ideal - 2, 1)
                        return {
                            "addr": addr,
                            "steamid": steamid,
                            "name": name,
                            # "region": server_region,
                            # "continent": continent,
                            # "country": country,
                            "players": num_players,
                            "max_players": max_players,
                            "bots": bots,
                            "map": map,
                            "gametype": list(gametype),
                            "score": score,
                            "point": [lon, lat],
                            "ping": overhead,
                        }

                    server_infos = await asyncio.gather(
                        *[calc_server(server) for server in pending_servers]
                    )
                    new_servers = [server for server in server_infos if server]
                    new_servers.sort(key=get_score, reverse=True)
                    with open("servers.json", "w") as fp:
                        json.dump(new_servers, fp)
                    if not DEBUG:
                        async with comfig_session.post(
                            "/api/quickplay/update",
                            headers={"Authorization": f"Bearer {COMFIG_API_KEY}"},
                            json={"servers": new_servers},
                        ) as api_resp:
                            print(await api_resp.text())
                    print(len(new_servers))
        except Exception:
            traceback.print_exc()

        print("Sleeping...")
        await asyncio.sleep(QUERY_INTERVAL + chaos(0.1))
        print("Continuing...")


def handle_geoip(geoipDb, edition):
    if geoipDb.exists():
        diff = utcnow().timestamp() - geoipDb.stat().st_mtime
        if diff / 24 / 3600 / 1000 <= 30:
            return True
    archive_name = f"./{edition}.tar.gz"
    urllib.request.urlretrieve(
        f"https://download.maxmind.com/app/geoip_download?edition_id={edition}&license_key={GEOIP_KEY}&suffix=tar.gz",
        archive_name,
    )
    with tarfile.open(archive_name) as tar:
        members = tar.getmembers()
        for member in members:
            if member.name.endswith(".mmdb"):
                with tar.extractfile(member) as db:
                    with open(geoipDb, "wb") as out:
                        out.write(db.read())
                break
    return True


async def main():
    geoipDb = Path(f"./GeoIP2-City.mmdb")
    handle_geoip(geoipDb, "GeoLite2-City")
    geoAsnDb = Path(f"./GeoIP2-ASN.mmdb")
    handle_geoip(geoAsnDb, "GeoLite2-ASN")
    with geoip2.database.Reader(geoAsnDb) as geoasn:
        with geoip2.database.Reader(geoipDb) as geoip:
            async with aiohttp.ClientSession(
                base_url="https://api.steampowered.com"
            ) as api_session:
                async with aiohttp.ClientSession(base_url=CDN_BASE_URL) as cdn_session:
                    async with aiohttp.ClientSession(
                        base_url="https://worker.comfig.app",
                        json_serialize=ujson.dumps,
                    ) as comfig_session:
                        await query_runner(
                            geoasn, geoip, api_session, cdn_session, comfig_session
                        )


def start():
    asyncio.run(main())
