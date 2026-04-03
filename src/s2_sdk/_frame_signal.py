class FrameSignal:
    """Tracks whether any HTTP/2 DATA frames were written to the socket.

    Signalled by the transport's ``on_write`` callback. An unsignalled
    state at retry time proves no request data left the process, making
    the retry safe. Reset at the start of each attempt.
    """

    __slots__ = ("_signalled",)

    def __init__(self) -> None:
        self._signalled = False

    def is_signalled(self) -> bool:
        return self._signalled

    def signal(self) -> None:
        self._signalled = True

    def reset(self) -> None:
        self._signalled = False
