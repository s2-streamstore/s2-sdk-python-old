import asyncio
import os
import random
from datetime import timedelta
from typing import AsyncIterable

from s2_sdk import S2, Batching, Record, append_inputs

ACCESS_TOKEN = os.getenv("S2_ACCESS_TOKEN")
MY_BASIN = os.getenv("MY_BASIN")
MY_STREAM = os.getenv("MY_STREAM")


async def records_gen() -> AsyncIterable[Record]:
    num_records = random.randint(1, 100)
    for _ in range(num_records):
        body_size = random.randint(1, 1024)
        if random.random() < 0.5:
            await asyncio.sleep(random.random() * 2.5)
        yield Record(body=os.urandom(body_size))


async def producer():
    async with S2(ACCESS_TOKEN) as s2:
        stream = s2[MY_BASIN][MY_STREAM]
        async with stream.append_session() as session:
            async for batch in append_inputs(
                records=records_gen(),
                batching=Batching(
                    max_records=10,
                    linger=timedelta(milliseconds=5),
                ),
            ):
                ticket = await session.submit(batch)
                ack = await ticket
                num_appended_records = ack.end.seq_num - ack.start.seq_num
                print(f"appended {num_appended_records} records")


if __name__ == "__main__":
    asyncio.run(producer())
