"""
유틸 모음집
"""

import tracemalloc
from pathlib import Path
from typing import Any, Union

import asyncio
import aiohttp
import xmltodict
import pandas as pd
from requests.exceptions import RequestException

from core.setting.create_log import SocketLogCustomer
from core.setting.properties import API_KEY, URL

tracemalloc.start()


class AsyncResponseDataFactory:
    """
    Response Factory
    """

    def __init__(self) -> None:
        self._logging = SocketLogCustomer()

    async def _xml_to_dict_convert(self, response: str) -> dict:
        """
        XML 문자열을 딕셔너리로 변환.

        Parameters:
        - xml_string (str): XML 형태의 문자열.

        Returns:
        - dict[str, Any]: XML을 딕셔너리로 변환한 결과.
        """
        return xmltodict.parse(response)

    async def _create_response(self, url: str) -> dict:
        """
        주어진 URL에 비동기 요청을 보내고 응답을 반환.

        Parameters:
        - url (str): API에 요청을 보낼 URL.

        Returns:
        - Any: XML 응답을 딕셔너리로 변환한 값.

        Raises:
        - RequestException: API 호출에 문제가 발생한 경우.
        """
        async with aiohttp.ClientSession() as session:
            response = await session.get(url)
            await asyncio.sleep(1)
            match response.status:
                case 200:
                    return await self._xml_to_dict_convert(await response.text())
                case _:
                    raise RequestException(
                        f"API 호출의 에러가 일어났습니다 --> {response.status}"
                    )

    async def async_congestion_response(
        self, location: str, city_type: str = "citydata_ppltn"
    ) -> dict[str, Union[str, dict[str, Any]]]:
        """
        주어진 위치에 대한 혼잡도 정보를 비동기로 요청

        매개변수:
            location (str): 혼잡도 정보를 요청할 지역의 이름.
            city_type (str, optional): 도시 데이터 타입. 기본값은 "citydata_ppltn".

        반환값:
            dict[str, Union[str, dict[str, Any]]]: 지정된 위치의 혼잡도 정보.
        """
        try:
            url = f"{URL}/{API_KEY}/xml/{city_type}/1/1000/{location}"

            # data -> dict[str.upper(), str]
            data: dict = await self._create_response(url=url)
            return data["Map"]["SeoulRtd.citydata_ppltn"]
        except Exception as error:
            await self._logging.error_log(
                error_type="connection_error",
                message=f"데이터 요청 중 오류 발생: {error}",
            )
            return {}


class SeoulPlaceClassifier:
    """
    카테고리 <---> 지역 매칭
    확장성 고려
    """

    def __init__(self) -> None:
        self.__csv_location: Path = Path(__file__).parent.parent

    def _get_english_category(self, korean_name: str) -> str:
        category_mapping = {
            "고궁·문화유산": "palace_and_cultural_heritage",
            "공원": "park",
            "관광특구": "tourist_special_zone",
            "발달상권": "developed_market",
            "인구밀집지역": "populated_area",
        }

        return category_mapping.get(korean_name, "unknown_topic")

    def place_classfier(self, filename="seoul_place.csv") -> dict[str, list[str]]:
        """
        카테고리별 지역 반환

        Parameters:
        - filename (str): 카테고리 와 지역이 저장된 CSV 파일의 경로. 기본값은 "config/seoul_place.csv".

        Returns:
        - dict[str, list[str]]: 카테고리별 지역 이름의 리스트를 값으로 하는 딕셔너리.
        """
        place_data = pd.read_csv(f"{self.__csv_location}/config/{filename}")

        return {
            self._get_english_category(category): data["AREA_NM"].to_list()
            for category, data in place_data.groupby("CATEGORY")
        }

    def seoul_place(self) -> dict[str, list[str]]:
        """
        카테고리별 지역 반환 (서울)
        """
        return SeoulPlaceClassifier().place_classfier()
