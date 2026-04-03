import asyncio
import os
import random

from s2_sdk import S2, Record

ACCESS_TOKEN = os.getenv("S2_ACCESS_TOKEN")
MY_BASIN = os.getenv("MY_BASIN")
MY_STREAM = os.getenv("MY_STREAM")


async def main():
    async with S2(ACCESS_TOKEN) as s2:
        stream = s2[MY_BASIN][MY_STREAM]
        async with stream.producer() as producer:
            for i in range(100):
                body_size = random.randint(1, 1024)
                ticket = await producer.submit(Record(body=os.urandom(body_size)))
                ack = await ticket
                print(f"seq_num={ack.seq_num}")


if __name__ == "__main__":
    asyncio.run(main())
