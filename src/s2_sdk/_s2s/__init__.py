from urllib.parse import quote


def _stream_records_path(stream_name: str) -> str:
    return f"/v1/streams/{quote(stream_name, safe='')}/records"
