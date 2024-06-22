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
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path

import a2s
import aiohttp
import geoip2
import geoip2.database
import geopy.distance
import orjson
import tinydb
import ujson
import vdf
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
GEOIP_KEY = os.getenv("GEOIP_KEY")
if not GEOIP_KEY:
    print("Need to pass in GEOIP_KEY")
    sys.exit(1)
STEAM_API_PARAM = {"key": STEAM_API_KEY, "format": "json"}
QUERY_INTERVAL = 10
QUERY_INTERVAL_VARIANCE = 5
QUERY_FILTER = r"\appid\440\gamedir\tf\secure\1\dedicated\1\ngametype\hidden,friendlyfire,highlander,noquickplay,trade,dmgspread,mvm,pve,gravity\steamblocking\1\nor\1\white\1"
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
DEBUG_SKIP_SERVERS = os.getenv("QUICKPLAY_DEBUG_SKIP_SERVERS") is not None

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
geo_table = DB.table("geo")
anycast_table = DB.table("anycast")

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

DEFAULT_GAMEMODES = set(
    [
        "attack_defense",
        "ctf",
        "capture_point",
        "koth",
        "payload",
        "payload_race",
        "alternative",
        "arena",
    ]
)

ANY_VALID_TAGS = set(
    [
        "cp",
        "ctf",
        "sd",
        "payload",
        "rd",
        "pd",
        "tc",
        "powerup",
        "passtime",
        "misc",
        "arena",
    ]
)

# these aren't in the GC (or are holiday only), but we still want them.
BASE_GAME_MAPS = {
    # beta maps
    "rd_asteroid": "alternative",
    "pl_cactuscanyon": "payload",
    # holiday maps
    "ctf_doublecross_snowy": "ctf",
    "ctf_haarp": "ctf",
    "ctf_frosty": "ctf",
    "ctf_snowfall_final": "ctf",
    "ctf_turbine_winter": "ctf",
    "cp_frostwatch": "attack_defense",
    "pl_frostcliff": "payload",
    "pl_chilly": "payload",
    "pl_coal_event": "payload",
    "pl_rumford_event": "payload",
    "pl_wutville_event": "payload",
    "plr_hacksaw": "payload_race",
    "koth_maple_ridge_event": "koth",
    "koth_megalo": "koth",
    "koth_snowtower": "koth",
    "pd_snowville_event": "alternative",
    "pd_galleria": "alternative",
    "arena_perks": "arena",
    # community map variants
    "cp_stoneyridge_rc2 ": "capture_point",
    "cp_ambush_rc5": "attack_defense",
    "pl_coal_rc23": "payload",
    "pl_fifthcurve_rc1": "payload",
    "pl_millstone_v4": "payload",
    "pl_rumford_rc2": "payload",
    "pl_rumble_rc1": "payload",
    "pl_sludgepit_final4": "payload",
    "pl_vineyard_rc8b": "payload",
    "koth_bagel_rc9b": "koth",
    "koth_brine_rc3a": "koth",
    "koth_maple_ridge_rc2": "koth",
    "koth_megasnow_rc1": "koth",
    "koth_moonshine": "koth",
    "koth_slaughter_rc1": "koth",
    "koth_synthetic_rc6a": "koth",
    "koth_undergrove_rc1": "koth",
    # popular community maps
    # this is an investigation to improve server pop
    "cp_glassworks_rc7a": "capture_point",
    "cp_mist_rc1e": "capture_point",
    "cp_overgrown_rc8": "capture_point",
    "cp_propaganda_b19": "capture_point",
    "cp_logjam_rc12": "capture_point",
    "cp_hazyfort_rc6": "capture_point",
    "pl_stallberg_rc3": "payload",
    "pl_vigil_rc10": "payload",
    "pl_oasis_rc3": "payload",
    "pl_rocksalt_v7": "payload",
    "pl_shoreleave_rc2": "payload",
    "pl_barnblitz_pro7": "payload",
    "pl_prowater_b12": "payload",
    "pl_badwater_pro_v12": "payload",
    "pl_kinder_b17": "payload",
    "pl_summercoast_rc8e": "payload",
    "pl_outback_rc4": "payload",
    "pl_cactuscanyon_redux_final2": "payload",
    "pl_extinction_rc3": "payload",
    "pl_patagonia_rc1b": "payload",
    "plr_tdm_hightower_rc1": "payload_race",
    "koth_hangar_rc5b": "koth",
    "koth_soot_final1": "koth",
    "koth_product_final": "koth",
    "koth_clearcut_b15d": "koth",
    "koth_ashville_final1": "koth",
    "koth_jamram_rc2b": "koth",
    "pd_salvador_b2": "alternative",
    # refresh.tf, used a lot on Asia servers
    "pl_upward_f12": "payload",
    "pl_borneo_f2": "payload",
    "cp_process_f12": "capture_point",
    "cp_metalworks_f5": "capture_point",
    "cp_gullywash_f9": "capture_point",
    "koth_warmtic_f10": "koth",
}

COMMUNITY_MAPS_UNVERSIONED = None
if DEBUG:
    COMMUNITY_MAPS_UNVERSIONED = set()
    for name in BASE_GAME_MAPS.keys():
        unversion_name_split = name.split("_")
        if len(unversion_name_split) > 2:
            unversion_name = "_".join(unversion_name_split[:-1])

BETA_MAPS = set(["rd_asteroid", "pl_cactuscanyon"])

DEFAULT_MAP_PREFIXES = set(["koth", "ctf", "cp", "tc", "pl", "plr", "sd", "pd"])

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
            body = await resp.json(encoding="utf-8")
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
                        items_game_body = await items_game_resp.text(encoding="utf-8")
                        updated = True
                        last_items_game_resp = vdf.loads(
                            items_game_body, mapper=vdf.VDFDict
                        )["items_game"]
        async with api_session.get(
            "/IGCVersion_440/GetServerVersion/v1/", params=STEAM_API_PARAM
        ) as resp:
            body = await resp.json(encoding="utf-8")
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
    new_total_players = new_humans

    real_max_players = max_players
    if max_players > FULL_PLAYERS:
        max_players = FULL_PLAYERS

    if new_humans > FULL_PLAYERS:
        # if it's actually full it'll get caught by the condition below.
        new_humans = FULL_PLAYERS - 1

    if new_total_players + SERVER_HEADROOM > real_max_players:
        return -100.0

    if new_humans == 1:
        return -0.3

    count_low = max_players // 3
    count_ideal = (max_players * 5) // 6

    score_low = 0.1
    score_ideal = 1.6
    score_fuller = 0.2

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
    server_params = {
        "key": STEAM_API_KEY,
        "format": "json",
        "limit": QUERY_LIMIT,
        "filter": QUERY_FILTER,
    }
    gamemodes: dict[str, set[str]] = {}
    map_gamemode: dict[str, str] = {}
    holiday_map_gamemode: dict[int, dict[str, str]] = defaultdict(dict)
    banned_ips = set(get_value("ips", default=[], table=ban_table))
    banned_ids = set(get_value("ids", default=[], table=ban_table))
    banned_name_search = get_value("names", default=[], table=ban_table)
    banned_tags = set(get_value("tags", default=[], table=ban_table))
    anycast_ips = set(get_value("ips", default=[], table=anycast_table))
    my_ip = "127.0.0.1"
    async with aiohttp.ClientSession("https://api.ipify.org") as ip_session:
        async with ip_session.get("/?format=json") as resp:
            body = await resp.json()
            my_ip = body["ip"]
    my_city = geoip.city(my_ip)
    my_lon = my_city.location.longitude
    my_lat = my_city.location.latitude
    my_point = (my_lat, my_lon)
    LAST_MONTH = 0
    pending_servers = []
    updated_servers = False
    while True:
        next_query_interval = QUERY_INTERVAL + chaos(QUERY_INTERVAL_VARIANCE)
        items_game, updated, server_version = await req_items_game(
            api_session, cdn_session
        )
        now = utcnow()
        month = now.month
        if month != LAST_MONTH:
            LAST_MONTH = month
            updated = True
        try:
            if items_game:
                if updated:
                    map_gamemode = dict(BASE_GAME_MAPS)
                    gamemodes = {}
                    holiday_map_gamemode = defaultdict(dict)
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
                        if mm_type not in valid_types and gamemode != "arena":
                            continue
                        restrictions = details.get("restrictions")
                        holiday_month = None
                        if restrictions:
                            for restriction, name in restrictions.items():
                                if restriction == "holiday":
                                    holiday_month = HOLIDAYS.get(name)
                        maplist = details["maplist"]
                        gamemode_maps = set()
                        if (
                            mm_type == "special_events" or mm_type == "alternative"
                        ) and gamemode != "payload_race":
                            gamemode = mm_type
                        for map_info in maplist.values():
                            name = map_info["name"]
                            enabled = map_info["enabled"] == "1" or gamemode == "arena"
                            if enabled:
                                gamemode_maps.add(name)
                                if holiday_month is not None and holiday_month != month:
                                    holiday_map_gamemode[holiday_month][name] = gamemode
                                else:
                                    if name not in map_gamemode:
                                        map_gamemode[name] = gamemode
                        if (
                            mm_type == "special_events" or mm_type == "alternative"
                        ) and gamemode != "payload_race":
                            if holiday_month is None or holiday_month == month:
                                if mm_type in gamemodes:
                                    gamemodes[mm_type].update(gamemode_maps)
                                else:
                                    gamemodes[mm_type] = gamemode_maps
                        else:
                            gamemodes[gamemode] = gamemode_maps
                    if not DEBUG:
                        async with comfig_session.post(
                            "/api/schema/update",
                            headers={"Authorization": f"Bearer {COMFIG_API_KEY}"},
                            json={
                                "schema": {
                                    "map_gamemodes": map_gamemode,
                                    "gamemodes": {
                                        k: list(v)
                                        for k, v in gamemodes.items()
                                        if k not in DEFAULT_GAMEMODES
                                    },
                                }
                            },
                        ) as api_resp:
                            print(await api_resp.text())

                try:
                    async with api_session.get(
                        "/IGameServersService/GetServerList/v1/",
                        params=server_params,
                    ) as resp:
                        body = await resp.read()
                        body = body.decode("utf-8", errors="replace")
                        body = orjson.loads(body)
                        pending_servers = body["response"]["servers"]
                        updated_servers = True
                except:
                    traceback.print_exc()

                async def calc_server(server):
                    addr = server["addr"]
                    # skip servers with SDR
                    if addr.startswith("169.254"):
                        if DEBUG and not DEBUG_SKIP_SERVERS:
                            return {
                                "score": -999,
                                "removal": "sdr",
                                "addr": addr,
                                "steamid": server["steamid"],
                                "name": server["name"],
                                "players": server["players"],
                                "max_players": server["max_players"],
                                "bots": server["bots"],
                                "map": server.get("map"),
                                "gametype": server.get("gametype", "")
                                .lower()
                                .split(","),
                            }
                        else:
                            return None
                    quickplay_bonus = 6
                    # check for steam ID
                    steamid = server["steamid"]
                    if steamid[0] == "9":
                        if False:
                            if DEBUG and not DEBUG_SKIP_SERVERS:
                                return {
                                    "score": -999,
                                    "removal": "nosteam",
                                    "addr": addr,
                                    "steamid": steamid,
                                    "name": server["name"],
                                    "players": server["players"],
                                    "max_players": server["max_players"],
                                    "bots": server["bots"],
                                    "map": server.get("map"),
                                    "gametype": server.get("gametype", "")
                                    .lower()
                                    .split(","),
                                }
                            else:
                                return None
                        else:
                            quickplay_bonus -= 0.1

                    if not updated_servers:
                        ip, port = addr.split(":")
                        if True:
                            try:
                                server_query = await a2s.ainfo((ip, port))
                            except:
                                return None
                            server["appid"] = server_query.app_id
                            server["gamedir"] = server_query.folder
                            server["product"] = server_query.folder
                            server["players"] = server_query.player_count
                            server["map"] = server_query.map_name
                            server["gametype"] = server_query.keywords
                            server["version"] = server_query.version
                        else:
                            server["appid"] = 440
                            server["gamedir"] = "tf"
                            server["product"] = "tf"
                            server["version"] = server_version
                            server["gametype"] = ",".join(server["gametype"])

                    # not tf, leave
                    if server["appid"] != APP_ID:
                        if DEBUG and not DEBUG_SKIP_SERVERS and False:
                            return {"score": -999, "removal": "noappid"}
                        else:
                            return None
                    if server["gamedir"] != APP_NAME:
                        if DEBUG and not DEBUG_SKIP_SERVERS and False:
                            return {"score": -999, "removal": "nogamedir"}
                        else:
                            return None
                    if server["product"] != APP_NAME:
                        if DEBUG and not DEBUG_SKIP_SERVERS and False:
                            return {"score": -999, "removal": "noprod"}
                        else:
                            return None
                    # check max players
                    max_players = server["max_players"]
                    if max_players < MIN_PLAYER_CAP:
                        # not enough max_players
                        if DEBUG and not DEBUG_SKIP_SERVERS:
                            return {
                                "score": -999,
                                "removal": "<18",
                                "addr": addr,
                                "steamid": steamid,
                                "name": server["name"],
                                "players": server["players"],
                                "max_players": server["max_players"],
                                "bots": server["bots"],
                                "map": server.get("map"),
                                "gametype": server.get("gametype", "")
                                .lower()
                                .split(","),
                            }
                        else:
                            return None
                    if max_players > MAX_PLAYER_CAP:
                        # too much max_players
                        if DEBUG and not DEBUG_SKIP_SERVERS:
                            return {"score": -999, "removal": ">101"}
                        else:
                            return None
                    num_players = server["players"]
                    if num_players >= max_players:
                        # lying about players
                        if DEBUG and not DEBUG_SKIP_SERVERS and False:
                            return {"score": -999, "removal": "playercaplie"}
                        else:
                            return None
                    # check if out of date
                    if int(server["version"]) < server_version:
                        if DEBUG and not DEBUG_SKIP_SERVERS and False:
                            return {"score": -999, "removal": "outofdate"}
                        else:
                            return None
                    # check if it's a casual map
                    map = server.get("map")
                    if not map:
                        return None
                    if map not in map_gamemode:
                        if DEBUG and not DEBUG_SKIP_SERVERS:
                            prefix = map.split("_")[0]
                            if prefix == "arena":
                                return {
                                    "score": -999,
                                    "removal": "arenamap",
                                    "addr": addr,
                                    "steamid": steamid,
                                    "name": server["name"],
                                    "players": server["players"],
                                    "max_players": server["max_players"],
                                    "bots": server["bots"],
                                    "map": map,
                                    "gametype": server.get("gametype", "")
                                    .lower()
                                    .split(","),
                                }
                            holiday = False
                            for map_lookup in holiday_map_gamemode.values():
                                if map in map_lookup:
                                    holiday = True
                                    break
                            if holiday:
                                return {
                                    "score": -999,
                                    "removal": "holidaymap",
                                    "addr": addr,
                                    "steamid": steamid,
                                    "name": server["name"],
                                    "players": server["players"],
                                    "max_players": server["max_players"],
                                    "bots": server["bots"],
                                    "map": map,
                                    "gametype": server.get("gametype", "")
                                    .lower()
                                    .split(","),
                                }
                            if prefix in DEFAULT_MAP_PREFIXES and not map.startswith(
                                "cp_orange"
                            ):
                                unversion_name_split = map.split("_")
                                if len(unversion_name_split) > 2:
                                    unversion_name = "_".join(unversion_name_split[:-1])
                                    if unversion_name in COMMUNITY_MAPS_UNVERSIONED:
                                        return {
                                            "score": -999,
                                            "removal": "versionmapdiff",
                                            "addr": addr,
                                            "steamid": steamid,
                                            "name": server["name"],
                                            "players": server["players"],
                                            "max_players": server["max_players"],
                                            "bots": server["bots"],
                                            "map": map,
                                            "gametype": server.get("gametype", "")
                                            .lower()
                                            .split(","),
                                        }

                                return {
                                    "score": -999,
                                    "removal": "custommap",
                                    "addr": addr,
                                    "steamid": steamid,
                                    "name": server["name"],
                                    "players": server["players"],
                                    "max_players": server["max_players"],
                                    "bots": server["bots"],
                                    "map": map,
                                    "gametype": server.get("gametype", "")
                                    .lower()
                                    .split(","),
                                }
                            return {
                                "score": -999,
                                "removal": "badmap",
                                "addr": addr,
                                "steamid": steamid,
                                "name": server["name"],
                                "players": server["players"],
                                "max_players": server["max_players"],
                                "bots": server["bots"],
                                "map": map,
                                "gametype": server.get("gametype", "")
                                .lower()
                                .split(","),
                            }
                        else:
                            return None
                    # check for ban
                    if server["steamid"] in banned_ids:
                        if DEBUG and not DEBUG_SKIP_SERVERS:
                            return {
                                "score": -999,
                                "removal": "steamban",
                                "addr": addr,
                                "steamid": steamid,
                                "name": server["name"],
                                "players": server["players"],
                                "max_players": server["max_players"],
                                "bots": server["bots"],
                                "map": map,
                                "gametype": server.get("gametype", "")
                                .lower()
                                .split(","),
                            }
                        else:
                            return None
                    ip, port = addr.split(":")
                    if ip in banned_ips:
                        if DEBUG and not DEBUG_SKIP_SERVERS:
                            return {
                                "score": -999,
                                "removal": "ipban",
                                "addr": addr,
                                "steamid": steamid,
                                "name": server["name"],
                                "players": server["players"],
                                "max_players": server["max_players"],
                                "bots": server["bots"],
                                "map": map,
                                "gametype": server.get("gametype", "")
                                .lower()
                                .split(","),
                            }
                        else:
                            return None
                    # check for gametype
                    gametype = server.get("gametype")
                    if not gametype:
                        return None
                    gametype = set(gametype.lower().split(","))
                    # is lying about max players?
                    if max_players > 25 and "increased_maxplayers" not in gametype:
                        if DEBUG and not DEBUG_SKIP_SERVERS:
                            return {
                                "score": -999,
                                "removal": "-maxplayers",
                                "addr": addr,
                                "steamid": steamid,
                                "name": server["name"],
                                "players": server["players"],
                                "max_players": server["max_players"],
                                "bots": server["bots"],
                                "map": map,
                                "gametype": list(gametype),
                            }
                        else:
                            return None
                    if max_players <= 24 and "increased_maxplayers" in gametype:
                        if DEBUG and not DEBUG_SKIP_SERVERS:
                            return {
                                "score": -999,
                                "removal": "+maxplayers",
                                "addr": addr,
                                "steamid": steamid,
                                "name": server["name"],
                                "players": server["players"],
                                "max_players": server["max_players"],
                                "bots": server["bots"],
                                "map": map,
                                "gametype": list(gametype),
                            }
                        else:
                            return None
                    # is it any of the gamemodes we want?
                    found_valid_gametype = (
                        len(gametype.intersection(ANY_VALID_TAGS)) > 0
                    )
                    if not found_valid_gametype:
                        if DEBUG and not DEBUG_SKIP_SERVERS:
                            return {
                                "score": -999,
                                "removal": "nogametype",
                                "addr": addr,
                                "steamid": steamid,
                                "name": server["name"],
                                "players": server["players"],
                                "max_players": server["max_players"],
                                "bots": server["bots"],
                                "map": map,
                                "gametype": list(gametype),
                            }
                        else:
                            return None
                    # is it the gamemode we want?
                    expected_gamemode = GAMEMODE_TO_TAG.get(map_gamemode[map])
                    if expected_gamemode and expected_gamemode not in gametype:
                        if DEBUG and not DEBUG_SKIP_SERVERS:
                            return {
                                "score": -999,
                                "removal": "unexpectedtag",
                                "addr": addr,
                                "steamid": steamid,
                                "name": server["name"],
                                "players": server["players"],
                                "max_players": server["max_players"],
                                "bots": server["bots"],
                                "map": map,
                                "gametype": list(gametype),
                            }
                        else:
                            return None
                    beta_expected = map in BETA_MAPS
                    if (
                        beta_expected
                        and "beta" not in gametype
                        or not beta_expected
                        and "beta" in gametype
                    ):
                        if DEBUG and not DEBUG_SKIP_SERVERS:
                            return {
                                "score": -999,
                                "removal": "nobeta" if beta_expected else "hasbeta",
                                "addr": addr,
                                "steamid": steamid,
                                "name": server["name"],
                                "players": server["players"],
                                "max_players": server["max_players"],
                                "bots": server["bots"],
                                "map": map,
                                "gametype": list(gametype),
                            }
                        else:
                            return None
                    # check for tag errors
                    found_valid_gametype = len(gametype.intersection(banned_tags)) < 1
                    if not found_valid_gametype:
                        if DEBUG and not DEBUG_SKIP_SERVERS:
                            return {
                                "score": -999,
                                "removal": "badgametype",
                                "addr": addr,
                                "steamid": steamid,
                                "name": server["name"],
                                "players": server["players"],
                                "max_players": server["max_players"],
                                "bots": server["bots"],
                                "map": map,
                                "gametype": list(gametype),
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
                        if DEBUG and not DEBUG_SKIP_SERVERS:
                            return {
                                "score": -999,
                                "removal": "badname",
                                "addr": addr,
                                "steamid": steamid,
                                "name": server["name"],
                                "players": server["players"],
                                "max_players": server["max_players"],
                                "bots": server["bots"],
                                "map": map,
                                "gametype": list(gametype),
                            }
                        else:
                            return None
                    bots = server["bots"]
                    rep = get_value(steamid, table=rep_table)
                    if rep is None:
                        rep = 0
                    score = rep + quickplay_bonus
                    score += score_server(num_players, bots, max_players)
                    if updated_servers:
                        try:
                            server_query = await a2s.ainfo((ip, port))
                        except:
                            if DEBUG and not DEBUG_SKIP_SERVERS:
                                return {
                                    "score": -999,
                                    "removal": "timeout",
                                    "addr": addr,
                                    "steamid": steamid,
                                    "name": server["name"],
                                    "players": server["players"],
                                    "max_players": server["max_players"],
                                    "bots": bots,
                                    "map": map,
                                    "gametype": list(gametype),
                                }
                            else:
                                return None
                    if server_query.password_protected:
                        if DEBUG and not DEBUG_SKIP_SERVERS and False:
                            return {"score": -999, "removal": "pass"}
                        else:
                            return None
                    if server_query.app_id != APP_ID:
                        return False
                    if server_query.game_id != APP_ID:
                        return False
                    if server_query.folder != APP_NAME:
                        return False
                    if server_query.game != APP_FULL_NAME:
                        if False:
                            if DEBUG and not DEBUG_SKIP_SERVERS:
                                return {
                                    "score": -999,
                                    "removal": "incorrectgame",
                                    "name": name,
                                    "game": server_query.game,
                                    "players": server["players"],
                                }
                            else:
                                return None
                        else:
                            score -= 0.1
                    # shift the scores around a little bit so we get some variance in sorting
                    if 0 < score <= 6.025:
                        score = shuffle(score, pct=0.0005)
                    # calculate ping score
                    ping = server_query.ping * 1000
                    geo_override = get_value(ip, table=geo_table)
                    if geo_override:
                        country = geo_override["country"]
                        continent = geo_override["continent"]
                        lon = geo_override["lon"]
                        lat = geo_override["lat"]
                    else:
                        city = geoip.city(ip)
                        country = city.country.iso_code
                        continent = city.continent.code
                        lon = city.location.longitude
                        lat = city.location.latitude
                    # TODO: do something with non-matching regions
                    server_region = server.get("region", 255)
                    point = (lat, lon)
                    asn = geoasn.asn(ip)
                    # aso = asn.autonomous_system_organization
                    asnn = str(asn.network)
                    if asnn in anycast_ips:
                        score -= 0.1
                    dist = geopy.distance.distance(my_point, point).km
                    # found through gradient descent
                    ideal = dist / 65.5
                    overhead = max(ping - ideal - 1, 1)
                    # strip attention seeking characters
                    if name.startswith("\u0001"):
                        score -= 0.1
                    name = (
                        name.replace("\u0001", "")
                        .replace("\t", "")
                        .encode("raw_unicode_escape")
                        .decode("unicode_escape")
                        .strip()
                    )
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
                pending_servers = new_servers
                updated_servers = False
                with open("servers.json", "w", encoding="utf-8") as fp:
                    json.dump(new_servers, fp, ensure_ascii=False, indent=2)
                if not DEBUG:
                    until = (
                        utcnow() + datetime.timedelta(seconds=next_query_interval + 1)
                    ).timestamp()
                    async with comfig_session.post(
                        "/api/quickplay/update",
                        headers={"Authorization": f"Bearer {COMFIG_API_KEY}"},
                        json={"servers": new_servers, "until": until},
                    ) as api_resp:
                        print(await api_resp.text())
                print(len(new_servers))
        except Exception:
            traceback.print_exc()

        print("Sleeping...")
        await asyncio.sleep(next_query_interval)
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
                base_url="https://api.steampowered.com", raise_for_status=True
            ) as api_session:
                async with aiohttp.ClientSession(
                    base_url=CDN_BASE_URL, raise_for_status=True
                ) as cdn_session:
                    async with aiohttp.ClientSession(
                        base_url=COMFIG_API_URL,
                        json_serialize=ujson.dumps,
                    ) as comfig_session:
                        await query_runner(
                            geoasn, geoip, api_session, cdn_session, comfig_session
                        )


def start():
    asyncio.run(main())
