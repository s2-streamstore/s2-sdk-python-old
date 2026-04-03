import asyncio
import os

from s2_sdk import S2, SeqNum

ACCESS_TOKEN = os.getenv("S2_ACCESS_TOKEN")
MY_BASIN = os.getenv("MY_BASIN")
MY_STREAM = os.getenv("MY_STREAM")


async def consumer():
    async with S2(ACCESS_TOKEN) as s2:
        stream = s2[MY_BASIN][MY_STREAM]
        tail = await stream.check_tail()
        print(f"reading from tail: {tail}")
        total_num_records = 0
        async for batch in stream.read_session(start=SeqNum(tail.seq_num)):
            total_num_records += len(batch.records)
            print(f"read {len(batch.records)} now, {total_num_records} so far")


if __name__ == "__main__":
    asyncio.run(consumer())
