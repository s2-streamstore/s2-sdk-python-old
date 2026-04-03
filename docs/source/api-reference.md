# API Reference

```{eval-rst}
.. module:: s2_sdk

.. autoclass:: S2
    :members:


.. autoclass:: S2Basin()
    :members:


.. autoclass:: S2Stream()
    :members:


.. autoclass:: AppendSession()
    :members:


.. autoclass:: BatchSubmitTicket()
    :members:

.. autoclass:: Producer()
    :members:


.. autoclass:: RecordSubmitTicket()
    :members:

.. autoclass:: Endpoints
    :members:


.. autoclass:: Timeout(request: timedelta = timedelta(seconds=5), connection: timedelta = timedelta(seconds=3))
    :members:

.. autoclass:: Retry(max_attempts: int = 3, min_base_delay: timedelta = timedelta(milliseconds=100), max_base_delay: timedelta = timedelta(seconds=1), append_retry_policy: AppendRetryPolicy = AppendRetryPolicy.ALL)
    :members:

.. autoclass:: Batching(max_records: int = 1000, max_bytes: int = 1048576, linger: timedelta = timedelta(milliseconds=5))
    :members:

.. autoclass:: Record(body: bytes, headers: list[tuple[bytes, bytes]] = [], timestamp: int | None = None)
    :members:

.. autoclass:: AppendInput
    :members:

.. autoclass:: AppendAck()
    :members:

.. autoclass:: IndexedAppendAck()
    :members:

.. autoclass:: StreamPosition()
    :members:

.. autoclass:: ReadLimit
    :members:

.. autoclass:: ReadBatch()
    :members:

.. autoclass:: SequencedRecord()
    :members:

.. autoclass:: SeqNum
    :members:

.. autoclass:: Timestamp
    :members:

.. autoclass:: TailOffset
    :members:

.. autoclass:: Page()
    :members:

.. autoclass:: CommandRecord()
    :members:


.. autofunction:: metered_bytes

.. autofunction:: append_record_batches

.. autofunction:: append_inputs

.. autoenum:: Compression

.. autoenum:: AppendRetryPolicy

.. autoenum:: StorageClass

.. autoenum:: TimestampingMode

.. autoclass:: Timestamping
    :members:

.. autoclass:: StreamConfig
    :members:

.. autoclass:: BasinConfig
    :members:

.. autoenum:: BasinScope

.. autoclass:: BasinInfo()
    :members:

.. autoclass:: StreamInfo()
    :members:

.. autoclass:: ExactMatch
    :members:

.. autoclass:: PrefixMatch
    :members:

.. autoenum:: Permission

.. autoenum:: Operation

.. autoclass:: OperationGroupPermissions
    :members:

.. autoclass:: AccessTokenScope(basins: ExactMatch | PrefixMatch | None = None, streams: ExactMatch | PrefixMatch | None = None, access_tokens: ExactMatch | PrefixMatch | None = None, op_groups: OperationGroupPermissions | None = None, ops: list[Operation] = [])
    :members:

.. autoclass:: AccessTokenInfo()
    :members:

.. autoenum:: MetricUnit

.. autoenum:: TimeseriesInterval

.. autoenum:: AccountMetricSet

.. autoenum:: BasinMetricSet

.. autoenum:: StreamMetricSet

.. autoclass:: Scalar()
    :members:

.. autoclass:: Accumulation()
    :members:

.. autoclass:: Gauge()
    :members:

.. autoclass:: Label()
    :members:

.. autoclass:: S2Error()
    :members:

.. autoclass:: S2ClientError()
    :members:
    :show-inheritance:

.. autoclass:: S2ServerError()
    :members:
    :show-inheritance:

.. autoclass:: AppendConditionError()
    :members:
    :show-inheritance:

.. autoclass:: FencingTokenMismatchError()
    :members:
    :show-inheritance:

.. autoclass:: SeqNumMismatchError()
    :members:
    :show-inheritance:

.. autoclass:: ReadUnwrittenError()
    :members:
    :show-inheritance:
```
