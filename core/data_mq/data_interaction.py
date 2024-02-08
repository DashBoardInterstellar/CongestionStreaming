"""
KAFAK PRODUCE 
"""
import json
from typing import Any
from pathlib import Path
from collections import defaultdict

from aiokafka import AIOKafkaProducer
from aiokafka.errors import (
    KafkaConnectionError,
    BrokerNotAvailableError,
    KafkaProtocolError,
)
from core.setting.create_log import log
from core.setting.properties import (
    BOOTSTRAP_SERVER,
    SECURITY_PROTOCOL,
    MAX_BATCH_SIZE,
    MAX_REQUEST_SIZE,
    ARCKS,
    deep_getsizeof,
)

present_path = Path(__file__).parent.parent
logging = log(
    log_location=f"{present_path}/log/kafka_message.log", name="messge_sending"
)


except_list = defaultdict(list)


async def produce_sending(topic: Any, message: Any, key: Any = None):
    """
    카프카 세팅값

    """
    config = {
        "bootstrap_servers": f"{BOOTSTRAP_SERVER}",
        "security_protocol": f"{SECURITY_PROTOCOL}",
        "max_batch_size": int(f"{MAX_BATCH_SIZE}"),
        "max_request_size": int(f"{MAX_REQUEST_SIZE}"),
        "acks": f"{ARCKS}",
        "key_serializer": lambda key: key.encode("utf-8"),
        "value_serializer": lambda value: json.dumps(value).encode("utf-8"),
        "retry_backoff_ms": 100,
    }
    producer = AIOKafkaProducer(**config)

    await producer.start()
    try:
        await producer.send_and_wait(topic=topic, value=message, key=key)
        size: int = deep_getsizeof(message)
        logging.info(
            "Message delivered to: %s --> counting --> %s size --> %s",
            topic,
            len(message),
            size,
        )

        # 불능 상태에서 저장된 메시지가 있는 경우 함께 전송
        while except_list[topic]:
            stored_message = except_list[topic].pop(0)
            await producer.send_and_wait(topic, stored_message)

    except (
        BrokerNotAvailableError,
        KafkaProtocolError,
        KafkaConnectionError,
    ) as error:
        logging.error(
            "Kafka broker error로 인해 임시 저장합니다 : %s, message: %s", error, message
        )
        except_list[topic].append(json.dumps(message).encode("utf-8"))
    finally:
        await producer.stop()
