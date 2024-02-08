"""
로그
"""

import logging
from datetime import datetime
from pathlib import Path


def log(name: str, log_location: str) -> logging.Logger:
    """
    주어진 이름으로 로거를 생성. 스트림 및 파일 핸들러가 존재하지 않는 경우 로거에 추가

    매개변수:
    - name: 로거의 이름.
    - log_location: 로그 파일을 저장할 경로.

    반환값:
    - 로거 객체.
    """
    logger = logging.getLogger(name=name)
    if logger.hasHandlers():
        return logger

    logger.propagate = False
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    try:
        file_handler = logging.FileHandler(filename=log_location)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except (FileNotFoundError, FileExistsError) as error:
        logger.error("Failed to create file handler due to: %s", error)
        # 파일이 없을 때 파일을 생성하도록 수정
        Path(log_location).parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(filename=log_location)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


class SocketLogCustomer:
    """
    Log 생성 정형화
    """

    def __init__(self, base_path: Path = None) -> None:
        """
        기본 또는 지정된 base_path로 SocketLogCustomer를 초기화

        매개변수:
        - base_path: 로그 파일이 저장될 경로.
        """
        if base_path:
            self.base_path = base_path
        else:
            self.base_path = Path(__file__).parent.parent.parent / "streaming" / "log"

    def create_logger(
        self, log_name: str, log_type: str, rate_type: str, nof: bool
    ) -> logging.Logger:
        """
        지정된 로그 이름과 유형을 사용하여 로거를 생성

        매개변수:
        - log_name: 로그 파일의 이름.
        - log_type: 로그의 유형 (connection, data 등).
        - rate_type: 혼잡도 타입 (AGE or GENDER).
        - noF: FCST_YN 플래그, 예보가 포함되어 있지 않은 경우 True.

        반환값:
        - 로거 객체.
        """
        try:
            return self._extracted_from_create_logger_18(
                log_name, nof, log_type, rate_type
            )
        except (FileNotFoundError, FileExistsError) as error:
            logger = logging.getLogger(__name__)
            logger.error("Failed to create or access the log file: %s", error)
            return logger

    # TODO Rename this here and in `create_logger`
    def _extracted_from_create_logger_18(self, log_name, noF, log_type, rate_type):
        # 현재 날짜를 년/월/일 형태로 가져옵니다.
        current_date = datetime.now().strftime("%Y/%m/%d")

        base_folder_name = log_name.split("_")[0]  # 예: popArea
        folder_name = "no_pred_log" if noF else "pred_log"
        # log_path에 날짜 및 상세 폴더 정보를 추가합니다.
        log_path = (
            self.base_path
            / current_date
            / log_type
            / base_folder_name
            / folder_name
            / f"{rate_type}_data.log"
        )

        # 경로에 필요한 모든 폴더를 생성합니다.
        log_path.parent.mkdir(parents=True, exist_ok=True)
        return log(log_name.split(".")[0], str(log_path))

    async def log_message(
        self,
        log_name: str,
        log_type: str,
        message: str,
        level: str = "info",
        rate_type: str = None,
        nof: bool = None,
    ):
        """
        지정된 로그 이름, 유형 및 레벨을 사용하여 메시지를 기록

        매개변수:
        - log_name: 로그 파일의 이름.
        - log_type: 로그의 유형 (connection, data 등).
        - message: 기록될 메시지.
        - level: 로깅 레벨 (기본값은 "info").
        - rate_type: 혼잡도 타입 (AGE or GENDER).
        - noF: FCST_YN 플래그, 예보가 포함되어 있지 않은 경우 True.
        """
        logger = self.create_logger(log_name, log_type, rate_type, nof)
        log_func = getattr(logger, level, "info")
        log_func(message)

    async def connection(
        self, location: str, message: str, rate_type: str, nof: bool
    ) -> None:
        """
        특정 장소 관련된 연결 메시지를 기록

        매개변수:
        - location: 장소의 이름.
        - message: 기록될 메시지.
        - rate_type: 혼잡도 타입 (AGE or GENDER).
        - noF: FCST_YN 플래그, 예보가 포함되어 있지 않은 경우 True.
        """
        log_name = f"{location}_connection.log"
        await self.log_message(
            log_name, "connection", message, rate_type=rate_type, nof=nof
        )

    async def data_log(
        self, location: str, message: str, rate_type: str, nof: bool
    ) -> None:
        """
        특정 장소 생성된 데이터 기록

        매개변수
        - location: 장소
        - message: 데이터 기록
        - rate_type: 혼잡도 타입 (AGE or GENDER).
        - noF: FCST_YN 플래그, 예보가 포함되어 있지 않은 경우 True.
        """
        log_name = f"{location}_data.log"
        await self.log_message(log_name, "data", message, rate_type=rate_type, nof=nof)

    async def error_log(self, error_type: str, message: str) -> None:
        """
        에러로그

        매개변수
        - error_type: 에러 타입
        - message: 메시지 타입
        """
        log_name = f"{error_type}.log"
        await self.log_message(log_name, "error", message, level="error")
