"""
------------------------------------------------------
|                                                    |   
| -- Seoul Age and Gender Congestion rate extract -- |
|                                                    |
------------------------------------------------------
"""

from __future__ import annotations

import logging
from pydantic import BaseModel, ValidationError
from core.setting.properties import (
    transform_data,
    get_congestion_value,
)


class BasePopulationRate(BaseModel):
    """공통 스키마"""

    category: str
    area_name: str
    ppltn_time: str
    area_congestion_lvl: int
    area_congestion_msg: str
    area_ppltn_min: int
    area_ppltn_max: int
    male_ppltn_rate: float
    female_ppltn_rate: float

    @staticmethod
    def _predict_yn(data: dict[str, dict[str, list[dict[str, float]]]]) -> dict:
        """Provide population prediction data if available."""
        if isinstance(data.get("FCST_YN"), dict):
            return transform_data(data["FCST_PPLTN"])

    @staticmethod
    def _rate_ppltn_extract(data: dict[str, str], keyword: str) -> dict[str, float]:
        """키워드에 따라서 데이터 추출

        Args:
            - data (dict[str, str]): 혼잡도 API
            - keyword (str): 추출할 키워드

        Returns:
            dict[str, float]:
            >>> {
            "ppltn_rate_0": 0.3,
            "ppltn_rate_10": 5.7,
            "ppltn_rate_20": 26.9,
            ...
            }
        """
        return {
            key.lower(): float(value) for key, value in data.items() if keyword in key
        }

    @classmethod
    def schema_extract(
        cls, category: str, data: dict[str, str], target: str, keyword: str
    ) -> BasePopulationRate:
        """공통스키마

        Args:
            data (dict[str, str]): 서울시 도시 실시간 인구 혼잡도 API
            target (str): 추출한 키
            keyword (str): 추출할 키워드

        Returns:
            >>> 각 스키마에 맞춰서
        """
        try:
            return cls(
                category=category,
                area_name=data["AREA_NM"],
                ppltn_time=data["PPLTN_TIME"],
                area_congestion_lvl=get_congestion_value(data["AREA_CONGEST_LVL"]),
                area_congestion_msg=data["AREA_CONGEST_MSG"],
                area_ppltn_min=int(data["AREA_PPLTN_MIN"]),
                area_ppltn_max=int(data["AREA_PPLTN_MAX"]),
                male_ppltn_rate=float(data["MALE_PPLTN_RATE"]),
                female_ppltn_rate=float(data["FEMALE_PPLTN_RATE"]),
                **{target.lower(): cls._rate_ppltn_extract(data, keyword)},
            ).model_dump()
        except ValidationError as error:
            logging.error("schem extract error --> %s", error)
            return None


# ------------------------------------------------------------------------------------------------------------#


class AgeCongestionSpecific(BaseModel):
    """각 나이대별 혼잡도 비율"""

    ppltn_rate_0: float
    ppltn_rate_10: float
    ppltn_rate_20: float
    ppltn_rate_30: float
    ppltn_rate_40: float
    ppltn_rate_50: float
    ppltn_rate_60: float
    ppltn_rate_70: float


class TotalAgeRateComposition(BasePopulationRate):
    """각 나이대별 혼잡도 스키마 만들기"""

    age_rate: AgeCongestionSpecific

    @classmethod
    def schema_modify(cls, category: str, data: dict[str, str]) -> BasePopulationRate:
        """
        Args:
            - data (dict[str, str]): 서울시 도시 실시간 인구 혼잡도 API

        Returns:
        >>> {
                "area_name": "가로수길",
                "area_congestion_lvl": "보통",
                "area_congestion_msg": "사람이 몰려있을 수 있지만 크게 붐비지는 않아요. 도보 이동에 큰 제약이 없어요.",
                "area_ppltn_min": 30000,
                "area_ppltn_max": 32000,
                "age_rate": {
                    "ppltn_rate_0": 0.3,
                    "ppltn_rate_10": 5.7,
                    "ppltn_rate_20": 26.9,
                    ~~~
                }
            }
        """
        return super().schema_extract(category, data, "age_rate", "PPLTN_RATE_")


# ------------------------------------------------------------------------------------------------------------#


class ForecastPopulation(BaseModel):
    """예측값"""

    fcst_time: float
    fcst_congest_lvl: int
    fcst_ppltn_min: float
    fcst_ppltn_max: float


class PredictFcst(BaseModel):
    """예측값"""

    fcst_ppltn: list[ForecastPopulation]


class AreaPredictSpecific(BasePopulationRate):

    fcst_yn: PredictFcst

    @staticmethod
    def _predict_yn(data: dict[str, dict[str, list[dict[str, float]]]]) -> dict:
        """Provide population prediction data if available."""
        if isinstance(data.get("FCST_YN"), dict):
            return transform_data(data["FCST_PPLTN"])

    @classmethod
    def schema_modify(cls, category: str, data: dict[str, str]) -> BasePopulationRate:
        """
        Args:
            - data (dict[str, str]): 서울시 도시 실시간 인구 혼잡도 API
            - rate_key (str): 추출한 키
            - keyword (str): 추출할 키워드\n
        Returns:
            >>> {
                    "area_name": "가로수길",
                    "area_congestion_lvl": "보통",
                    "area_congestion_msg": "사람이 몰려있을 수 있지만 크게 붐비지는 않아요. 도보 이동에 큰 제약이 없어요.",
                    "area_ppltn_min": 30000,
                    "area_ppltn_max": 32000,
                    "fcst_yn": {
                        "fcst_ppltn": [
                            {
                                "fcst_time": 1693998000.0,
                                "fcst_congest_lvl": 2,
                                "fcst_ppltn_min": 28000.0,
                                "fcst_ppltn_max": 30000.0,
                            },
                        ]
                    }
                }
        """
        return super().schema_extract(category, data, "gender_rate", "E_PPLTN_RATE")
