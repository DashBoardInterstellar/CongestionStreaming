"""
async congestion
"""

import tracemalloc
from typing import Any

from aiokafka.errors import KafkaConnectionError

from core.data_mq.data_interaction import produce_sending
from core.congestion_response.abstract_class import (
    AbstractSeoulDataSending,
    AbstractDataTransfore,
)
from core.congestion_response.utils import (
    AsyncResponseDataFactory as ARDF,
    SeoulPlaceClassifier as Seoul,
)
from core.congestion_response.data_format import (
    TotalAgeRateComposition as TRC,
    AreaGenderRateSpecific as AGRS,
)


tracemalloc.start()


class AsyncSeoulCongestionDataSending(AbstractSeoulDataSending):
    """Data Response"""

    def __init__(self, strategy: AbstractDataTransfore) -> None:
        super().__init__()
        self._strategy = strategy

    async def async_data_sending(
        self, congest: dict[str, Any], category: str, location: str, rate_type: str
    ) -> None:
        """데이터 전송 로직
        Args:
            - congest (dict[str, Any]): 혼잡도 데이터
            - category (str): 지역
            - location (str): 장소
            - rate_type (str): 혼잡도 타입
        """
        # 토픽명 변환 로직
        topic_transform = {
            "developed_market": "devMkt",
            "palace_and_cultural_heritage": "palCult",
            "park": "park",
            "populated_area": "popArea",
            "tourist_special_zone": "tourZone",
        }

        transformed_category = topic_transform.get(category)
        rate_schema: dict = self._strategy.transform(category, congest)
        try:
            match congest["FCST_YN"]:
                case "Y":
                    await produce_sending(
                        topic=f"{transformed_category}_{rate_type}",
                        message=rate_schema,
                        key=location,
                    )
                    await self.logging.data_log(
                        location=f"{transformed_category}_{rate_type}",
                        rate_type=rate_type,
                        message=rate_schema,
                        nof=False,
                    )
        except KafkaConnectionError as error:
            self.logging.error_log(
                error_type="kafka_connection",
                message=f"Kafk 데이터 전송 실패 --> {error}",
            )

    async def async_popular_congestion(self, rate_type: str) -> None:
        """혼잡도 데이터를 기반으로 적절한 토픽에 데이터를 전송

        매개변수:
            congest (dict[str, Any]): 혼잡도 데이터.
            category (str): 지역.
            location (str): 장소.
            rate_type (str): 혼잡도 타입.
        """
        while True:
            try:
                for category, location in Seoul().seoul_place().items():
                    for data in location:
                        congestion = await ARDF().async_congestion_response(
                            location=data
                        )
                        await self.async_data_sending(
                            category=category,
                            location=data,
                            rate_type=rate_type,
                            congest=congestion,
                        )

            except KafkaConnectionError as error:
                await self.logging.error_log(error_type="NotConnection", message=error)


class AgeCongestionRate(AbstractDataTransfore):
    """나이별 혼잡도 클래스"""

    def transform(self, category: str, data: dict[str, Any]) -> dict[str, Any]:
        return TRC.schema_modify(category, data)


class GenderCongestionRate(AbstractDataTransfore):
    """성별 혼잡도 클래스"""

    def transform(self, category: str, data: dict[str, Any]) -> dict[str, Any]:
        return AGRS.schema_modify(category, data)
