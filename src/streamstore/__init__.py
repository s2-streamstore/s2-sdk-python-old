import warnings

warnings.warn(
    "The 'streamstore' package is deprecated and will no longer be maintained. "
    "Migrate to the new package: pip install s2-sdk — "
    "see https://pypi.org/project/s2-sdk/ for details.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "S2",
    "Basin",
    "Stream",
    "S2Error",
    "streamstore.schemas",
    "streamstore.utils",
]

from streamstore._client import S2, Basin, Stream  # noqa: E402
from streamstore._exceptions import S2Error  # noqa: E402
