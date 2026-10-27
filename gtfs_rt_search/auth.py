"""
Authentication for feeds whose URLs carry an inline credential.

Some warehouse URIs are templates holding a placeholder, for example

    https://api.511.org/transit/vehiclepositions?api_key={{ MTC_511_API_KEY}}&agency=EM

The text inside the braces is the Google Cloud Secret Manager secret id, used exactly as written
once surrounding whitespace is stripped - no prefixing, no case changes. Note the real warehouse
spacing is lopsided (`{{ NAME}}`), which is why the pattern tolerates whitespace on either side.

Every secret value resolved here is registered for redaction, so it cannot reach a parquet file, a
poll log, or the console. Redaction works two ways:

* by value, masking any registered secret wherever it appears, whatever the parameter is called
* by parameter name, as a fallback for credentials this module never resolved

Keeping both matters: the value-based pass catches a key in a parameter we do not recognise, and
the name-based pass catches a key that was baked into the warehouse URI rather than resolved.
"""

from __future__ import annotations

import os
import re
import threading
from typing import Any, Optional

DEFAULT_SECRET_PROJECT = "cal-itp-data-infra"
SECRET_PROJECT_ENV_VAR = "GTFS_RT_SECRET_PROJECT"

# {{ NAME }} with optional whitespace either side. The id charset matches Secret Manager's own
# (letters, digits, hyphen, underscore) plus dots, which some naming schemes use.
PLACEHOLDER_RE = re.compile(r"\{\{\s*([A-Za-z0-9_.\-]+)\s*\}\}")

# Query parameters whose values are credentials, for the name-based fallback pass.
_SECRET_PARAM_RE = re.compile(
    r"([?&](?:api[_-]?key|key|token|access[_-]?token|secret|password|passwd|auth[a-z_]*)=)([^&\s]+)",
    re.IGNORECASE,
)

MASK = "***"

# Shorter values are not registered for value-based masking: a two-character "secret" would match
# everywhere and turn ordinary text into mush.
MIN_REGISTERED_SECRET_LENGTH = 6

_registered_secrets: set[str] = set()
_registry_lock = threading.Lock()


class SecretResolutionError(RuntimeError):
    """Raised when a URL placeholder cannot be turned into a credential."""


def placeholder_names(text: Optional[str]) -> list[str]:
    """Secret ids referenced by a URL, in order, de-duplicated."""
    if not text:
        return []
    seen: list[str] = []
    for match in PLACEHOLDER_RE.finditer(text):
        name = match.group(1)
        if name not in seen:
            seen.append(name)
    return seen


def has_placeholder(text: Optional[str]) -> bool:
    """True when a URL still contains an unsubstituted {{ }} block."""
    return bool(text) and "{{" in text


def register_secret_value(value: str) -> None:
    """Remember a secret value so redact() masks it wherever it later appears."""
    if not value or len(value) < MIN_REGISTERED_SECRET_LENGTH:
        return
    with _registry_lock:
        _registered_secrets.add(value)


def clear_registered_secrets() -> None:
    """Forget every registered value. For tests; the registry is process-wide."""
    with _registry_lock:
        _registered_secrets.clear()


def redact(text: Optional[str]) -> Optional[str]:
    """
    Mask credentials in text: registered secret values first, then known parameter names.

    Safe to call more than once - masking an already-masked string changes nothing.
    """
    if not text:
        return text

    with _registry_lock:
        # Longest first, so a secret that contains another is masked whole rather than in pieces.
        values = sorted(_registered_secrets, key=len, reverse=True)
    for value in values:
        if value in text:
            text = text.replace(value, MASK)

    return _SECRET_PARAM_RE.sub(r"\1" + MASK, text)


class SecretResolver:
    """
    Reads credentials from Secret Manager and substitutes them into feed URLs.

    Values are cached per secret id, because many feeds share one key - 46 of the current
    vehicle_positions feeds reference MTC_511_API_KEY - and each resolution is an API call.
    """

    def __init__(self, project: Optional[str] = None, client: Any = None, version: str = "latest"):
        self.project = project or os.environ.get(SECRET_PROJECT_ENV_VAR) or DEFAULT_SECRET_PROJECT
        self.version = version
        self._client = client
        self._cache: dict[str, str] = {}
        self._lock = threading.Lock()

    def _get_client(self) -> Any:
        if self._client is None:
            try:
                from google.cloud import secretmanager
            except ImportError as exc:  # pragma: no cover - depends on the environment
                raise SecretResolutionError(
                    "google-cloud-secret-manager is not installed; cannot resolve URL placeholders"
                ) from exc
            self._client = secretmanager.SecretManagerServiceClient()
        return self._client

    def secret_value(self, name: str) -> str:
        """The secret's value, read once and cached. Raises SecretResolutionError on any failure."""
        with self._lock:
            if name in self._cache:
                return self._cache[name]

        resource = f"projects/{self.project}/secrets/{name}/versions/{self.version}"
        try:
            response = self._get_client().access_secret_version(request={"name": resource})
            value = response.payload.data.decode("utf-8").strip()
        except SecretResolutionError:
            raise
        except Exception as exc:  # noqa: BLE001 - surface any backend failure as one error type
            # Redacted, since a backend message can echo back whatever it was given.
            detail = redact(f"{type(exc).__name__}: {exc}")
            raise SecretResolutionError(
                f"could not read secret {name!r} from project {self.project!r}: {detail}"
            ) from exc

        if not value:
            raise SecretResolutionError(f"secret {name!r} in project {self.project!r} is empty")

        register_secret_value(value)
        with self._lock:
            self._cache[name] = value
        return value

    def resolve(self, url: str) -> str:
        """
        Substitute every {{ NAME }} in url with its secret value.

        Raises SecretResolutionError if any referenced secret cannot be read, rather than
        returning a half-substituted URL that would be fetched and fail confusingly.
        """
        return PLACEHOLDER_RE.sub(lambda m: self.secret_value(m.group(1)), url)
