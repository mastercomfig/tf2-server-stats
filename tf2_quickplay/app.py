import asyncio
import datetime
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
import cachetools
import geoip2
import geoip2.database
import geopy.distance
import orjson
import tinydb
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
TEAMWORK_API_KEY = os.getenv("TEAMWORK_API_KEY")
if not TEAMWORK_API_KEY:
    print("Need to pass in TEAMWORK_API_KEY")
    sys.exit(1)
STEAM_API_PARAM = {"key": STEAM_API_KEY, "format": "json"}
QUERY_INTERVAL = 10
QUERY_INTERVAL_VARIANCE = 5
QUERY_FILTER = r"\appid\440\gamedir\tf\secure\1\dedicated\1\ngametype\hidden,friendlyfire,noquickplay,trade,dmgspread,mvm,pve,gravity\steamblocking\1\nor\1\white\1"
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
FULL_PLAYERS = 24

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

COMMUNITY_TAG_TO_OFFICIAL = {
    "pass": "passtime",
    "controlpoints": "cp",
    "pl_": "payload",
    "plr": "payload",
    "plr_": "payload",
    "payloadrace": "payload",
    "payload race": "payload",
    "plr_hightower": "payload",
    "plr_hi": "payload",
    "koth": "cp",
    "capture_the_flag": "ctf",
    "flag": "ctf",
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
    "ctf_snowfall_final": "ctf",
    "ctf_turbine_winter": "ctf",
    "cp_carrier": "attack_defense",
    "cp_frostwatch": "attack_defense",
    "cp_gravelpit_snowy": "attack_defense",
    "pl_frostcliff": "payload",
    "pl_chilly": "payload",
    "pl_coal_event": "payload",
    "pl_wutville_event": "payload",
    "koth_maple_ridge_event": "koth",
    "koth_megalo": "koth",
    "pd_snowville_event": "alternative",
    "pd_galleria": "alternative",
    "arena_perks": "arena",
    "plr_cutter": "payload",
    "vsh_maul": "alternative",
    "vsh_outburst": "alternative",
    "cp_freaky_fair": "alternative",
    # community map variants
    "cp_stoneyridge_rc2": "capture_point",
    "cp_ambush_rc5": "attack_defense",
    "pl_fifthcurve_rc1": "payload",
    "pl_millstone_v4": "payload",
    "pl_rumble_rc1": "payload",
    "pl_sludgepit_final4": "payload",
    "pl_vineyard_rc8b": "payload",
    "pl_eruption_b13": "payload",
    "pl_eruption_b14": "payload",
    "koth_bagel_rc10": "koth",
    "koth_brine_rc3a": "koth",
    "koth_camp_saxton_b1": "koth",
    "koth_maple_ridge_rc2": "koth",
    "koth_megasnow_rc1": "koth",
    "koth_moonshine": "koth",
    "koth_slaughter_rc1": "koth",
    "koth_synthetic_rc6a": "koth",
    "koth_undergrove_rc1": "koth",
    # popular community maps
    # this is an investigation to improve server pop
    "ctf_snowfort_2023": "ctf",
    "ctf_2fort_improved_a8f1": "ctf",
    "ctf_damnable_a4": "ctf",
    "cp_glassworks_rc7a": "capture_point",
    "cp_mist_rc1e": "capture_point",
    "cp_propaganda_b19": "capture_point",
    "cp_logjam_rc12": "capture_point",
    "cp_hazyfort_rc6": "capture_point",
    "cp_sultry_b8a": "capture_point",
    "pl_stallberg_rc3": "payload",
    "pl_vigil_rc10": "payload",
    "pl_oasis_rc3": "payload",
    "pl_rocksalt_v7": "payload",
    "pl_divulgence_b4b": "payload",
    "pl_metropolis_b7": "payload",
    "pl_highwood_b20": "payload",
    "pl_midwest_rc1a": "payload",
    "pl_shoreleave_rc2": "payload",
    "pl_barnblitz_pro6": "payload",
    "pl_barnblitz_pro7": "payload",
    "pl_prowater_b12": "payload",
    "pl_badwater_pro_v12": "payload",
    "pl_kinder_b17": "payload",
    "pl_summercoast_rc8e": "payload",
    "pl_outback_rc4": "payload",
    "pl_cactuscanyon_redux_final2": "payload",
    "pl_extinction_rc3": "payload",
    "koth_lakeside_r2": "koth",
    "koth_hangar_rc5b": "koth",
    "koth_soot_final1": "koth",
    "koth_product_final": "koth",
    "koth_clearcut_b15d": "koth",
    "koth_jamram_rc2b": "koth",
    "rd_asteroid_redux_b1": "alternative",
    "pd_salvador_b3c": "alternative",
    "plr_tdm_hightower_rc1": "alternative",
    "plr_highertower": "alternative",
    "plr_highertower_extended": "alternative",
    # refresh.tf, used a lot on Asia servers
    "cp_process_f12": "capture_point",
    "cp_process_f11": "capture_point",
    "cp_metalworks_f5": "capture_point",
    "cp_gullywash_f9": "capture_point",
    "cp_steel_f12": "attack_defense",
    "pl_upward_f12": "payload",
    "pl_upward_f10": "payload",
    "pl_borneo_f2": "payload",
    "koth_warmtic_f10": "koth",
    # some more custom misc maps
    "vsh_facility_rc4": "alternative",
    "vsh_graygravelhq_rc0": "alternative",
    "vsh_harvest_final": "alternative",
    "vsh_hightower_rc1": "alternative",
    "vsh_brewery_v4_fixed": "alternative",
    # popular 100-player server maps
    "pl_dustbowl_st3": "payload",
    "pl_circle_st1": "payload",
    "pl_dbz_b5": "payload",
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

PREFIX_TO_GAMEMODE = {
    "ctf": "ctf",
    "cp": "capture_point",
    "koth": "koth",
    "pl": "payload",
    "plr": "payload_race",
    "tc": "alternative",
    "sd": "alternative",
    "pd": "alternative",
}

TIMESTAMP_TIMEZONE = datetime.timezone.utc

player_count_history = cachetools.TTLCache(maxsize=4000, ttl=60 * 60)

PLAYER_TREND_MIN = 0.5
PLAYER_TREND_MAX = 0.85

PLAYER_TREND_COUNT_LOW_POINT_LIMIT = 12
PLAYER_TREND_COUNT_MAX = 18

CLASS_BAN_LIKELY = [
    "sniper banned",
    "sniper-free",
    "sniper is banned",
    "no sniper",
    "sniper-only",
]
NO_CAP_LIKELY_GAMETYPE = [
    "dm",
    "tdm",
    "duel",
    "noflag",
    "noflags",
    "nocart",
    "nocarts",
    "deathmatch",
]
NO_CAP_LIKELY_NAME = [
    "no flag",
    "no intel",
    "no cart",
    "deathmatch",
    "duel",
    "tdm",
]

shuffle_score_history = cachetools.TTLCache(maxsize=4000, ttl=60 * 60)


def utcnow() -> datetime.datetime:
    return datetime.datetime.now(tz=TIMESTAMP_TIMEZONE)


updated_thumbnails = False
update_thumbnails = False

MAP_THUMBNAILS: dict[str, str] = {}
with open("map_thumbnails.json", "rb") as fp:
    MAP_THUMBNAILS = orjson.loads(fp.read())

THUMBNAIL_OVERRIDES = {
    # official maps with missing thumbnails
    "cp_canaveral_5cp": "https://wiki.teamfortress.com/w/images/3/39/Cp_canaveral_5cp.png",
    "pd_atom_smash": "https://wiki.teamfortress.com/w/images/c/c5/Pd_atom_smash.png",
    "pl_precipice_event_final": "https://wiki.teamfortress.com/w/images/1/13/Precipice_main.png",
    "pd_mannsylvania": "https://wiki.teamfortress.com/w/images/0/09/Pd_mannsylvania.png",
    "pl_odyssey": "https://wiki.teamfortress.com/w/images/4/48/Odyssey.png",
    "ctf_turbine_winter": "https://wiki.teamfortress.com/w/images/b/ba/Ctf_turbine_winter.png",
    "cp_carrier": "https://wiki.teamfortress.com/w/images/6/69/Cp_carrier.png",
    "pl_vineyard": "https://wiki.teamfortress.com/w/images/5/52/Pl_vineyard.png",
    "pl_eruption": "https://wiki.teamfortress.com/w/images/9/99/Pl_eruption.jpg",
    "pl_embargo": "https://wiki.teamfortress.com/w/images/d/d3/Embargo.png",
    "arena_perks": "https://wiki.teamfortress.com/w/images/2/2e/Arena_perks.png",
    "cp_brew": "https://wiki.teamfortress.com/w/images/3/32/Cp_brew.png",
    "cp_fortezza": "https://wiki.teamfortress.com/w/images/d/d7/Cp_fortezza.png",
    "cp_hardwood_final": "https://wiki.teamfortress.com/w/images/1/1e/Cp_hardwood.png",
    "cp_hadal": "https://wiki.teamfortress.com/w/images/1/15/Cp_hadal.png",
    "ctf_applejack": "https://wiki.teamfortress.com/w/images/0/0b/Ctf_applejack.png",
    "ctf_haarp": "https://wiki.teamfortress.com/w/images/4/42/Ctf_haarp.png",
    "ctf_snowfall_final": "https://wiki.teamfortress.com/w/images/d/d0/Ctf_snowfall_final.png",
    "koth_cachoeira": "https://wiki.teamfortress.com/w/images/c/c0/Cachoeira.png",
    "koth_megaton": "https://steamuserimages-a.akamaihd.net/ugc/2524912417369499775/A55CECB1743214A09B0EC0DE2D7561A5591C3B4E/?imw=5000&imh=5000&ima=fit&impolicy=Letterbox&imcolor=%23000000&letterbox=false",
    "koth_overcast_final": "https://wiki.teamfortress.com/w/images/thumb/8/8f/Koth_overcast_final.png/1024px-Koth_overcast_final.png",
    "koth_snowtower": "https://wiki.teamfortress.com/w/images/4/47/Koth_snowtower.png",
    "pd_galleria": "https://wiki.teamfortress.com/w/images/0/0c/Pd_galleria.png",
    "pl_camber": "https://wiki.teamfortress.com/w/images/5/53/Pl_camber.png",
    "pl_cashworks": "https://wiki.teamfortress.com/w/images/3/32/Pl_cashworks.png",
    "pl_patagonia": "https://wiki.teamfortress.com/w/images/f/fa/Pl_patagonia.png",
    "plr_cutter": "https://wiki.teamfortress.com/w/images/f/f6/Plr_cutter.png",
    "plr_hacksaw": "https://wiki.teamfortress.com/w/images/5/5c/Plr_hacksaw.png",
    "vsh_maul": "https://wiki.teamfortress.com/w/images/a/a8/Vsh_maul.png",
    "vsh_outburst": "https://wiki.teamfortress.com/w/images/8/85/Vsh_outburst.png",
    # community maps
    "pl_sludepit_final4": "https://wiki.teamfortress.com/w/images/2/27/Sludgepit4.png",
    "koth_slaughter_rc1": "https://wiki.teamfortress.com/w/images/1/19/Slaughter_Main_Point.png",
    "plr_highertower_extended": "https://steamuserimages-a.akamaihd.net/ugc/939447123270412695/075848DC806626956889F2768D4E6D184ECC9CDB/?imw=5000&imh=5000&ima=fit&impolicy=Letterbox&imcolor=%23000000&letterbox=false",
    "koth_camp_saxton_b1": "https://steamuserimages-a.akamaihd.net/ugc/2046366098090407374/2C88F1674AC73A5D1F5C97717277946E3B8BEE4E/?imw=5000&imh=5000&ima=fit&impolicy=Letterbox&imcolor=%23000000&letterbox=false",
    "koth_synthetic_rc6a": "https://wiki.teamfortress.com/w/images/b/b1/Koth_synthetic.jpg",
    "cp_mist_rc1e": "https://tf2maps.net/attachments/20190316043533_1-jpg.97918/",
    "pl_highwood_b20": "https://tf2maps.net/attachments/20200831084050_1-jpg.132468/",
    "pl_rocksalt_v7": "https://tf2maps.net/attachments/rocksalt_screenshot_06-jpg.206209/",
    "pl_divulgence_b4b": "https://tf2maps.net/attachments/20220819102834_1-jpg.185416/",
    "pl_stallberg_rc3": "https://tf2maps.net/attachments/1-jpg.109719/",
    "pl_oasis_rc3": "https://tf2maps.net/attachments/20180418214049_1-jpg.76498/",
    "cp_sultry_b8a": "https://steamuserimages-a.akamaihd.net/ugc/2049744430627396988/E93F0C177FA9A5347D5FC8C8E6F71E210B14FFB7/?imw=5000&imh=5000&ima=fit&impolicy=Letterbox&imcolor=%23000000&letterbox=false",
    "cp_hazyfort_rc6": "https://tf2maps.net/attachments/20180415084648_1-jpg.75808/",
    "cp_logjam_rc12": "https://steamuserimages-a.akamaihd.net/ugc/448458176288634169/9E5D28B8DB7A40EA08BE43103FB282B8822D8D89/?imw=5000&imh=5000&ima=fit&impolicy=Letterbox&imcolor=%23000000&letterbox=false",
    "koth_megasnow_rc1": "https://tf2maps.net/attachments/20221030173951_1-jpg.190824/",
    "koth_brine_rc3a": "https://wiki.teamfortress.com/w/images/0/07/Koth_Brine.jpg",
    "koth_clearcut_b15d": "https://steamuserimages-a.akamaihd.net/ugc/1027329992977108536/A13BB4E9B77AACF0E27DB6171D6EE3D9817540C1/?imw=5000&imh=5000&ima=fit&impolicy=Letterbox&imcolor=%23000000&letterbox=false",
    "koth_soot_final1": "https://tf2maps.net/attachments/20220618174907_1-jpg.175883/",
    "pd_salvador_b3c": "https://tf2maps.net/attachments/screenshot_2-jpg.254618/",
    "pl_circle_st1": "https://steamuserimages-a.akamaihd.net/ugc/2496761664824305897/F74017E958E012C1C228B72389837FC3A18B4AE4/?imw=5000&imh=5000&ima=fit&impolicy=Letterbox&imcolor=%23000000&letterbox=false",
    "pl_extinction_rc3": "https://tf2maps.net/attachments/4-6-23-pl-extinction-a1-png.203654/",
    "pl_kinder_b17": "https://tf2maps.net/attachments/20190216141025_1-jpg.96293/",
    "pl_midwest_rc1a": "https://tf2maps.net/attachments/20220505124957_1-jpg.173535/",
    "pl_shoreleave_rc2": "https://tf2maps.net/attachments/20230129131135_1-jpg.198212/",
    "pl_summercoast_rc8e": "https://steamuserimages-a.akamaihd.net/ugc/1027329120122636877/4D1A505B0415FFAEEB418B95FB9C9AA75B0BC50E/?imw=5000&imh=5000&ima=fit&impolicy=Letterbox&imcolor=%23000000&letterbox=false",
    "vsh_facility_rc4": "https://tf2maps.net/attachments/1-jpg.234751/",
    "vsh_graygravelhq_rc0": "https://steamuserimages-a.akamaihd.net/ugc/2469736339902168708/107831A973AAC970BD8278182DA9BA13D25C2D27/?imw=5000&imh=5000&ima=fit&impolicy=Letterbox&imcolor=%23000000&letterbox=false",
    "vsh_hightower_rc1": "https://tf2maps.net/attachments/1-jpg.226290/",
}

for k, v in THUMBNAIL_OVERRIDES.items():
    if k not in MAP_THUMBNAILS or MAP_THUMBNAILS[k] != v:
        updated_thumbnails = True
        MAP_THUMBNAILS[k] = v

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

THUMBNAIL_UPDATE_INTERVAL = datetime.timedelta(hours=24)


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
            body = await resp.read()
            body = orjson.loads(body)
            new_overview_resp: str | None = body.get("result", EMPTY_DICT).get(
                "items_game_url"
            )
            if new_overview_resp:
                new_overview_resp = new_overview_resp.replace(
                    "http://media.steampowered.com", ""
                )
                next_overview_resp_time = current_time + OVERVIEW_INTERVAL + chaos()
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
            body = await resp.read()
            body = orjson.loads(body)
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


def to_nearest_even(num: float):
    return 2 * round(num / 2)


def score_server(humans: int, max_players: int) -> float:
    new_humans = humans + 1
    new_total_players = new_humans

    real_max_players = max_players
    if new_total_players + SERVER_HEADROOM > real_max_players:
        return -100.0

    # aim to give every game a base population normalized to 24 max players
    if max_players > FULL_PLAYERS:
        max_players = FULL_PLAYERS

    if new_humans > FULL_PLAYERS:
        # if it's actually full it'll get caught by the headroom condition above.
        new_humans = FULL_PLAYERS - 1

    # penalize a completely empty server
    if humans == 0:
        return -0.3

    # get 1/3, round to nearest even for balanced teams
    count_low = to_nearest_even(max_players / 3)
    # get 72% (1/2 sqrt(2)), round to nearest even for balanced teams
    # this gets us: 18 (9v9) for 24 players, and 12 (6v6) for 18 players
    count_ideal = to_nearest_even(max_players * 0.72)

    score_low = 0.1
    score_ideal = 1.6
    score_fuller = 0.2

    if new_humans <= count_low:
        return lerp(0, count_low, 0.0, score_low, new_humans)
    elif new_humans <= count_ideal:
        return lerp(count_low, count_ideal, score_low, score_ideal, new_humans)
    elif new_humans <= max_players:
        # give all servers equal footing for the slots they can compete for
        return lerp(count_ideal, max_players, score_ideal, score_fuller, new_humans)
    else:
        # score within the real bounds of the server, so we still give a bonus but less than our ideal 24 player match
        return lerp(max_players, real_max_players, score_fuller, score_low, new_humans)


async def query_runner(
    geoasn: geoip2.database.Reader,
    geoip: geoip2.database.Reader,
    api_session: aiohttp.ClientSession,
    cdn_session: aiohttp.ClientSession,
    comfig_session: aiohttp.ClientSession,
    teamwork_session: aiohttp.ClientSession,
):
    global updated_thumbnails
    global update_thumbnails
    server_params = {
        "key": STEAM_API_KEY,
        "format": "json",
        "limit": QUERY_LIMIT,
        "filter": QUERY_FILTER,
    }
    gamemodes: dict[str, set[str]] = {}
    map_gamemode: dict[str, str] = dict(BASE_GAME_MAPS)
    holiday_map_gamemode: dict[int, dict[str, str]] = defaultdict(dict)
    banned_ips = set(get_value("ips", default=[], table=ban_table))
    banned_ids = set(get_value("ids", default=[], table=ban_table))
    banned_name_search = get_value("names", default=[], table=ban_table)
    banned_tags = set(get_value("tags", default=[], table=ban_table))
    anycast_ips = set(get_value("ips", default=[], table=anycast_table))
    my_ip = "127.0.0.1"
    async with aiohttp.ClientSession("https://api.ipify.org") as ip_session:
        async with ip_session.get("/?format=json") as resp:
            body = await resp.read()
            body = orjson.loads(body)
            my_ip = body["ip"]
    my_city = geoip.city(my_ip)
    my_lon = my_city.location.longitude
    my_lat = my_city.location.latitude
    my_point = (my_lat, my_lon)
    LAST_MONTH = 0
    pending_servers = []
    updated_servers = False

    last_thumbnails_update = utcnow() - datetime.timedelta(hours=24)

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
                    update_thumbnails = True
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

                    map_gamemode = dict(sorted(map_gamemode.items()))

                if not update_thumbnails:
                    # if we aren't forcing an update because of a schema update, then we need to determine if we need to update otherwise
                    is_missing = False
                    for name in map_gamemode.keys():
                        if name not in MAP_THUMBNAILS or not MAP_THUMBNAILS[name]:
                            is_missing = True
                            break

                    if is_missing:
                        if (
                            utcnow() - last_thumbnails_update
                            > THUMBNAIL_UPDATE_INTERVAL
                        ):
                            update_thumbnails = True

                if update_thumbnails:
                    update_thumbnails = False
                    last_thumbnails_update = utcnow()
                    for name in map_gamemode.keys():
                        # if we don't have the map yet, or it's null
                        if name not in MAP_THUMBNAILS or not MAP_THUMBNAILS[name]:
                            async with teamwork_session.get(
                                f"/api/v1/map-stats/mapimages/{name}",
                                params={"key": TEAMWORK_API_KEY},
                            ) as resp:
                                body = await resp.read()
                                try:
                                    body = orjson.loads(body)
                                    err = body.get("error")
                                    if err:
                                        if DEBUG:
                                            print(err, name)
                                        continue
                                    MAP_THUMBNAILS[name] = body["thumbnail"]
                                    if not MAP_THUMBNAILS[name]:
                                        screenshots = body["screenshots"]
                                        if not screenshots:
                                            continue
                                        MAP_THUMBNAILS[name] = screenshots[0]
                                    updated_thumbnails = True
                                except:
                                    # bail out of the update due to errors
                                    break

                if updated_thumbnails:
                    with open("map_thumbnails.json", "wb") as fp:
                        fp.write(
                            orjson.dumps(MAP_THUMBNAILS, option=orjson.OPT_INDENT_2)
                        )

                if updated or updated_thumbnails:
                    if not DEBUG:
                        async with comfig_session.post(
                            "/api/schema/update",
                            headers={"Authorization": f"Bearer {COMFIG_API_KEY}"},
                            json={
                                "schema": {
                                    "map_gamemodes": map_gamemode,
                                    "map_thumbnails": MAP_THUMBNAILS,
                                    "gamemodes": {
                                        k: list(v)
                                        for k, v in gamemodes.items()
                                        if k not in DEFAULT_GAMEMODES
                                    },
                                }
                            },
                        ) as api_resp:
                            print(await api_resp.text())

                updated_thumbnails = False

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
                            server["players"] = (
                                server_query.player_count - server_query.bot_count
                            )
                            server["bots"] = server_query.bot_count
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
                    # normalize name
                    name = server["name"]
                    lower_name = name.lower()
                    # check for gametype
                    gametype = server.get("gametype")
                    if not gametype:
                        return None
                    gametype = set(gametype.lower().split(","))
                    if "rtd" not in gametype:
                        if "rtd" in lower_name:
                            gametype.add("rtd")
                    if "highlander" in gametype:
                        gametype.add("classlimits")
                    if "classlimit" in gametype:
                        gametype.add("classlimits")
                        gametype.remove("classlimit")
                    if "classban" in gametype:
                        gametype.add("classbans")
                        gametype.remove("classban")
                    if "classbans" not in gametype:
                        if any((x in lower_name for x in CLASS_BAN_LIKELY)):
                            gametype.add("classbans")
                    if "classlimits" not in gametype:
                        if "uncletopia" in lower_name:
                            gametype.add("classlimits")
                    if any((x in gametype for x in NO_CAP_LIKELY_GAMETYPE)):
                        gametype.add("nocap")
                    if "nocap" not in gametype:
                        if any((x in lower_name for x in NO_CAP_LIKELY_NAME)):
                            gametype.add("nocap")
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
                    if not expected_gamemode:
                        prefix = map.split("_")[0]
                        prefix_gamemode = PREFIX_TO_GAMEMODE.get(prefix)
                        if prefix_gamemode:
                            expected_gamemode = GAMEMODE_TO_TAG.get(prefix_gamemode)
                    # we let forced arena as an exception to this
                    if (
                        expected_gamemode
                        and expected_gamemode not in gametype
                        and "arena" not in gametype
                    ):
                        if DEBUG and not DEBUG_SKIP_SERVERS:
                            return {
                                "score": -999,
                                "removal": "missingexpectedtag",
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
                    if expected_gamemode:
                        # we checked if we have the tag we expect. now, let's check if we have tags we DON'T expect.
                        # if we have arena, powerups active, or misc active, that's fine
                        # but servers CANNOT double dip on gamemode search for players
                        expected_tags = set(
                            [expected_gamemode, "arena", "powerup", "misc"]
                        )
                        for tag in gametype:
                            tag = COMMUNITY_TAG_TO_OFFICIAL.get(tag, tag)
                            if tag not in ANY_VALID_TAGS:
                                continue
                            if tag in expected_tags:
                                continue
                            if DEBUG and not DEBUG_SKIP_SERVERS:
                                return {
                                    "score": -999,
                                    "removal": f"unexpectedtag",
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
                    score += score_server(num_players, max_players)
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
                        return None
                    if server_query.game_id != APP_ID:
                        return None
                    if server_query.folder != APP_NAME:
                        return None
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
                    # the lowest player count in the past hour
                    prev_player_count = player_count_history.get(steamid, None)
                    if prev_player_count is not None:
                        if num_players < prev_player_count:
                            player_count_history[steamid] = num_players
                        else:
                            if (
                                prev_player_count < PLAYER_TREND_COUNT_LOW_POINT_LIMIT
                                and num_players < PLAYER_TREND_COUNT_MAX
                            ):
                                player_increase = num_players - prev_player_count
                                if player_increase > 0:
                                    if (
                                        num_players
                                        >= PLAYER_TREND_COUNT_LOW_POINT_LIMIT
                                    ):
                                        score += PLAYER_TREND_MAX
                                    else:
                                        score += lerp(
                                            0,
                                            PLAYER_TREND_COUNT_LOW_POINT_LIMIT,
                                            PLAYER_TREND_MIN,
                                            PLAYER_TREND_MAX,
                                            num_players,
                                        )
                    else:
                        player_count_history[steamid] = num_players
                    # shift the scores around a little bit so we get some variance in sorting
                    shuffle_score = shuffle_score_history.get(steamid, None)
                    if num_players == 0:
                        if shuffle_score is None:
                            shuffle_score = shuffle(score, pct=0.0005) - score
                            shuffle_score_history[steamid] = shuffle_score
                        score += shuffle_score
                    elif shuffle_score is not None:
                        del shuffle_score_history[steamid]
                    # calculate ping score
                    ping = server_query.ping * 1000
                    geo_override = get_value(ip, table=geo_table)
                    if geo_override:
                        country = geo_override["country"]
                        continent = geo_override["continent"]
                        lon = geo_override["lon"]
                        lat = geo_override["lat"]
                    else:
                        try:
                            city = geoip.city(ip)
                        except:
                            if DEBUG:
                                traceback.print_exc()
                            return None
                        country = city.country.iso_code
                        continent = city.continent.code
                        lon = city.location.longitude
                        lat = city.location.latitude
                    # TODO: do something with non-matching regions
                    server_region = server.get("region", 255)
                    point = (lat, lon)
                    try:
                        asn = geoasn.asn(ip)
                        # aso = asn.autonomous_system_organization
                        asnn = str(asn.network)
                        if asnn in anycast_ips:
                            score -= 0.1
                    except geoip2.errors.AddressNotFoundError:
                        if DEBUG:
                            print(f"{ip} not in ASN database, passing")
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
                        .replace(r"\N", "")
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
                with open("servers.json", "wb") as fp:
                    fp.write(orjson.dumps(new_servers, option=orjson.OPT_INDENT_2))
                if not DEBUG:
                    until = (
                        utcnow() + datetime.timedelta(seconds=next_query_interval + 1)
                    ).timestamp()
                    async with comfig_session.post(
                        "/api/quickplay/update",
                        headers={"Authorization": f"Bearer {COMFIG_API_KEY}"},
                        json={"servers": new_servers, "until": until * 1000},
                    ) as api_resp:
                        print(await api_resp.text())
                print(len(new_servers))
                if DEBUG and not DEBUG_SKIP_SERVERS:
                    print(
                        len(
                            [
                                server
                                for server in new_servers
                                if get_score(server) > -200
                            ]
                        )
                    )
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


def encode_json(obj):
    return orjson.dumps(obj).decode("utf-8")


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
                        json_serialize=encode_json,
                    ) as comfig_session:
                        async with aiohttp.ClientSession(
                            base_url="https://teamwork.tf"
                        ) as teamwork_session:
                            await query_runner(
                                geoasn,
                                geoip,
                                api_session,
                                cdn_session,
                                comfig_session,
                                teamwork_session,
                            )


def start():
    asyncio.run(main())
