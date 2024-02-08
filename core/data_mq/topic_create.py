"""
토픽생성
"""
from core.data_mq.data_admin import new_topic_initialization


from core.setting.properties import (
    DEVMKT_AGE,
    PALCULT_AGE,
    PARK_AGE,
    POPAREA_AGE,
    TOURZONE_AGE,
    DEVMKT_NOF_AGE,
    PALCULT_NOF_AGE,
    PARK_NOF_AGE,
    POPAREA_NOF_AGE,
    TOURZONE_NOF_AGE,
)

from core.setting.properties import (
    DEVMKT_NOF_GENDER,
    PALCULT_NOF_GENDER,
    PARK_NOF_GENDER,
    POPAREA_NOF_GENDER,
    TOURZONE_NOF_GENDER,
    DEVMKT_GENDER,
    PALCULT_GENDER,
    PARK_GENDER,
    POPAREA_GENDER,
    TOURZONE_GENDER,
)
from core.setting.properties import (
    AVG_AGE_TOPIC,
    AVG_GENDER_TOPIC,
    AVG_N_AGE_TOPIC,
    AVG_N_GENDER_TOPIC,
)


def create_topic() -> None:
    """
    Topic create
    """
    topic = [
        DEVMKT_AGE,
        PALCULT_AGE,
        PARK_AGE,
        POPAREA_AGE,
        TOURZONE_AGE,
        DEVMKT_NOF_AGE,
        PALCULT_NOF_AGE,
        PARK_NOF_AGE,
        POPAREA_NOF_AGE,
        TOURZONE_NOF_AGE,
        DEVMKT_GENDER,
        PALCULT_GENDER,
        PARK_GENDER,
        POPAREA_GENDER,
        TOURZONE_GENDER,
        DEVMKT_NOF_GENDER,
        PALCULT_NOF_GENDER,
        PARK_NOF_GENDER,
        POPAREA_NOF_GENDER,
        TOURZONE_NOF_GENDER,
        AVG_AGE_TOPIC,
        AVG_GENDER_TOPIC,
        AVG_N_AGE_TOPIC,
        AVG_N_GENDER_TOPIC,
    ]
    partition: list[int] = [3 for _ in range(len(topic))]
    replication: list[int] = [3 for _ in range(len(topic))]

    return new_topic_initialization(
        topic=topic, partition=partition, replication_factor=replication
    )
