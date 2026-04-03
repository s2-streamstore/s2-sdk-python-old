import asyncio
import os
import random

from s2_sdk import S2, AppendInput, Record

ACCESS_TOKEN = os.getenv("S2_ACCESS_TOKEN")
MY_BASIN = os.getenv("MY_BASIN")
MY_STREAM = os.getenv("MY_STREAM")


async def producer():
    async with S2(ACCESS_TOKEN) as s2:
        stream = s2[MY_BASIN][MY_STREAM]
        async with stream.append_session() as session:
            num_inputs = random.randint(1, 100)
            for _ in range(num_inputs):
                num_records = random.randint(1, 100)
                records = []
                for _ in range(num_records):
                    body_size = random.randint(1, 1024)
                    records.append(Record(body=os.urandom(body_size)))
                ticket = await session.submit(AppendInput(records))
                ack = await ticket
                num_appended_records = ack.end.seq_num - ack.start.seq_num
                print(f"appended {num_appended_records} records")
                if random.random() < 0.5:
                    await asyncio.sleep(random.random() * 2.5)


if __name__ == "__main__":
    asyncio.run(producer())
