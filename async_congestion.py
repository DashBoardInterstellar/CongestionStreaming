"""
TEST
"""

if __name__ == "__main__":
    import asyncio
    from core.data_mq.topic_create import create_topic

    from core.data_mq.data_admin import delete_all_topics
    from core.congestion_response.seoul_congestion_api import (
        AsyncSeoulCongestionDataSending as ADS,
    )
    from core.congestion_response.seoul_congestion_api import AgeCongestionRate

    async def main() -> None:
        """main"""
        try:
            # delete_all_topics()
            create_topic()
            # from core.data_mq.s3_sink_connect import sink_connection

            task = [
                asyncio.create_task(
                    ADS(AgeCongestionRate()).async_popular_congestion("AGE")
                ),
            ]

            await asyncio.gather(*task)
        except Exception as error:
            print(error)

    # asyncio를 이용해 메인 함수를 실행
    asyncio.run(main())
