"""
API에 필요한것들
"""

import time
import sys
import configparser
from enum import Enum
from pathlib import Path
from typing import Any
from datetime import datetime


path = Path(__file__).parent.parent
parser = configparser.ConfigParser()
parser.read(f"{path}/config/setting.conf")


# 서울 API
API_KEY = parser.get("API", "key")
URL = parser.get("API", "url")

# KAFKA
BOOTSTRAP_SERVER = parser.get("KAFKA", "bootstrap_servers")
SECURITY_PROTOCOL = parser.get("KAFKA", "security_protocol")
MAX_BATCH_SIZE: int = parser.get("KAFKA", "max_batch_size")
MAX_REQUEST_SIZE: int = parser.get("KAFKA", "max_request_size")
ARCKS = parser.get("KAFKA", "acks")


# AGE TOPIC
DEVMKT_AGE = parser.get("AGETOPIC", "dev_market_AGE")
PALCULT_AGE = parser.get("AGETOPIC", "palace_culture_AGE")
PARK_AGE = parser.get("AGETOPIC", "park_AGE")
POPAREA_AGE = parser.get("AGETOPIC", "pop_area_AGE")
TOURZONE_AGE = parser.get("AGETOPIC", "tourist_zone_AGE")

DEVMKT_NOF_AGE = parser.get("AGETOPIC", "dev_market_noFCST_AGE")
PALCULT_NOF_AGE = parser.get("AGETOPIC", "palace_culture_noFCST_AGE")
PARK_NOF_AGE = parser.get("AGETOPIC", "park_noFCST_AGE")
POPAREA_NOF_AGE = parser.get("AGETOPIC", "pop_area_noFCST_AGE")
TOURZONE_NOF_AGE = parser.get("AGETOPIC", "tourist_zone_noFCST_AGE")

# ------------------------------------------------------------------------------

# GENDER TOPIC
DEVMKT_GENDER = parser.get("GENDERTOPIC", "dev_market_GENDER")
PALCULT_GENDER = parser.get("GENDERTOPIC", "palace_culture_GENDER")
PARK_GENDER = parser.get("GENDERTOPIC", "park_GENDER")
POPAREA_GENDER = parser.get("GENDERTOPIC", "pop_area_GENDER")
TOURZONE_GENDER = parser.get("GENDERTOPIC", "tourist_zone_GENDER")

DEVMKT_NOF_GENDER = parser.get("GENDERTOPIC", "dev_market_noFCST_GENDER")
PALCULT_NOF_GENDER = parser.get("GENDERTOPIC", "palace_culture_noFCST_GENDER")
PARK_NOF_GENDER = parser.get("GENDERTOPIC", "park_noFCST_GENDER")
POPAREA_NOF_GENDER = parser.get("GENDERTOPIC", "pop_area_noFCST_GENDER")
TOURZONE_NOF_GENDER = parser.get("GENDERTOPIC", "tourist_zone_noFCST_GENDER")


# ------------------------------------------------------------------------------

AVG_AGE_TOPIC = parser.get("AVGTOPIC", "avg_age_topic")
AVG_GENDER_TOPIC = parser.get("AVGTOPIC", "avg_gender_topic")
AVG_N_AGE_TOPIC = parser.get("AVGTOPIC", "avg_n_age_topic")
AVG_N_GENDER_TOPIC = parser.get("AVGTOPIC", "avg_n_gender_topic")

"""
-------------------------
|                       |   
| -- Seoul Properties-- |
|                       |
-------------------------
"""


# 메모리 계산
def deep_getsizeof(obj, seen=None) -> int:
    """재귀적으로 객체의 메모리 사용량을 계산하는 함수"""
    if seen is None:
        seen = set()

    obj_id = id(obj)
    if obj_id in seen:
        return 0

    # 이미 본 객체는 저장
    seen.add(obj_id)

    size = sys.getsizeof(obj)

    if isinstance(obj, dict):
        size += sum(deep_getsizeof(v, seen) for v in obj.values())
        size += sum(deep_getsizeof(k, seen) for k in obj.keys())
    elif hasattr(obj, "__iter__") and not isinstance(obj, (str, bytes, bytearray)):
        size += sum(deep_getsizeof(i, seen) for i in obj)

    return size


class CongestionLevel(Enum):
    """혼잡도 레벨 지정"""

    여유 = 0
    보통 = 1
    약간 = 2
    붐빔 = 3


def get_congestion_value(input_string: str) -> int:
    """혼잡도 레벨필터링"""
    if "약간" in input_string:
        return CongestionLevel.약간.value
    try:
        return CongestionLevel[input_string].value
    except KeyError:
        return 2


def utc_time(location_time: str) -> float:
    """utc time float transfor"""
    # 문자열을 datetime 객체로 변환
    date_time = datetime.strptime(location_time, "%Y-%m-%d %H:%M")
    # datetime 객체를 유닉스 타임스탬프로 변환
    return time.mktime(date_time.timetuple())


# 대문자 소문자 변환
def transform_data(obj) -> list | dict | Any:
    """
    대문자를 소문자로 재귀호출

    parameter
        - obj: dict or list comfact\n

    return:
    >>> {
        "area_name": "가로수길",
        "area_congestion_lvl": "보통",
        "area_congestion_msg": "사람이 몰려있을 수 있지만 크게 붐비지는 않아요. 도보 이동에 큰 제약이 없어요.",
        "area_ppltn_min": 30000,
        "area_ppltn_max": 32000,
        "fcst": "N",
        "age_congestion_specific": {
            "ppltn_rate_0": 0.3,
            "ppltn_rate_10": 5.7,
            ~~
        },
    }

    """
    if isinstance(obj, list):
        return [transform_data(data) for data in obj]
    if isinstance(obj, dict):
        return _extracted_from_transform_data_32(obj, transform_data)
    return obj


def _extracted_from_transform_data_32(obj, data) -> dict:
    new_obj = {k.lower(): data(v) for k, v in obj.items()}
    # fcst_ppltn_min 및 fcst_ppltn_max의 값을 실수로 변환
    if "fcst_ppltn_min" in new_obj:
        new_obj["fcst_ppltn_min"] = float(new_obj["fcst_ppltn_min"])
    if "fcst_ppltn_max" in new_obj:
        new_obj["fcst_ppltn_max"] = float(new_obj["fcst_ppltn_max"])
    if "fcst_time" in new_obj:
        new_obj["fcst_time"] = utc_time(new_obj["fcst_time"])
    if "fcst_congest_lvl" in new_obj:
        new_obj["fcst_congest_lvl"] = get_congestion_value(new_obj["fcst_congest_lvl"])
    return new_obj


normal_topic_age = [
    DEVMKT_AGE,
    PALCULT_AGE,
    PARK_AGE,
    POPAREA_AGE,
    TOURZONE_AGE,
]
normal_topic_gender = [
    DEVMKT_GENDER,
    PALCULT_GENDER,
    PARK_GENDER,
    POPAREA_GENDER,
    TOURZONE_GENDER,
]
normal_topic_no_age = [
    DEVMKT_NOF_AGE,
    PALCULT_NOF_AGE,
    PARK_NOF_AGE,
    POPAREA_NOF_AGE,
    TOURZONE_NOF_AGE,
]
normal_topic_no_gender = [
    DEVMKT_NOF_GENDER,
    PALCULT_NOF_GENDER,
    PARK_NOF_GENDER,
    POPAREA_NOF_GENDER,
    TOURZONE_NOF_GENDER,
]
