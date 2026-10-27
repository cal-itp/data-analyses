import pytest
from auth import (
    DEFAULT_SECRET_PROJECT,
    MIN_REGISTERED_SECRET_LENGTH,
    SECRET_PROJECT_ENV_VAR,
    SecretResolutionError,
    SecretResolver,
    clear_registered_secrets,
    has_placeholder,
    placeholder_names,
    redact,
    register_secret_value,
)

REAL_511_URI = "https://api.511.org/transit/vehiclepositions?api_key={{ MTC_511_API_KEY}}&agency=EM"


@pytest.fixture(autouse=True)
def _clean_registry():
    """The secret registry is process-wide, so no test may leak values into another."""
    clear_registered_secrets()
    yield
    clear_registered_secrets()


# -- placeholder parsing ---------------------------------------------------------------


def test_parses_the_lopsided_spacing_the_warehouse_actually_uses():
    # Real warehouse URIs are written "{{ NAME}}" - space before, none after.
    assert placeholder_names(REAL_511_URI) == ["MTC_511_API_KEY"]


@pytest.mark.parametrize(
    "url",
    [
        "https://x/vp?k={{NAME}}",
        "https://x/vp?k={{ NAME }}",
        "https://x/vp?k={{  NAME}}",
        "https://x/vp?k={{NAME  }}",
    ],
)
def test_whitespace_around_the_name_is_ignored(url):
    assert placeholder_names(url) == ["NAME"]


def test_secret_id_is_used_exactly_as_written():
    """The id is the secret name in Secret Manager verbatim - no casing or prefixing applied."""
    url = "https://x/vp?a={{ Mixed_Case-Name.v2 }}"
    assert placeholder_names(url) == ["Mixed_Case-Name.v2"]


def test_multiple_placeholders_are_returned_in_order_without_duplicates():
    url = "https://x/vp?a={{ FIRST }}&b={{ SECOND }}&c={{ FIRST }}"
    assert placeholder_names(url) == ["FIRST", "SECOND"]


def test_no_placeholders():
    assert placeholder_names("https://x/vp?agency=EM") == []
    assert placeholder_names(None) == []
    assert placeholder_names("") == []


def test_has_placeholder():
    assert has_placeholder(REAL_511_URI)
    assert not has_placeholder("https://x/vp")
    assert not has_placeholder(None)


# -- redaction -------------------------------------------------------------------------


def test_registered_value_is_masked_whatever_the_parameter_is_called():
    """The value-based pass is what catches a key in a parameter name we do not know."""
    register_secret_value("s3cret-value-xyz")
    masked = redact("https://x/vp?some_unknown_param=s3cret-value-xyz&agency=EM")
    assert "s3cret-value-xyz" not in masked
    assert "***" in masked
    assert "agency=EM" in masked


def test_known_parameter_names_are_masked_even_when_the_value_is_unregistered():
    for param in ("api_key", "API_KEY", "apikey", "api-key", "token", "access_token", "secret", "auth"):
        masked = redact(f"https://x/vp?{param}=never-registered")
        assert "never-registered" not in masked, param


def test_non_credential_parameters_are_left_alone():
    assert redact("https://x/vp?agency=EM&format=pb") == "https://x/vp?agency=EM&format=pb"


def test_very_short_values_are_not_registered():
    """A two-character 'secret' would match everywhere and mangle ordinary text."""
    short = "a" * (MIN_REGISTERED_SECRET_LENGTH - 1)
    register_secret_value(short)
    assert redact(f"a harmless {short} string") == f"a harmless {short} string"


def test_longest_registered_value_is_masked_first():
    """A secret containing another must be masked whole, not left as a half-masked fragment."""
    register_secret_value("abc123")
    register_secret_value("abc123456789")
    masked = redact("key=abc123456789")
    assert masked == "key=***"


def test_redaction_is_idempotent():
    register_secret_value("s3cret-value-xyz")
    once = redact("https://x/vp?api_key=s3cret-value-xyz")
    assert redact(once) == once


def test_redact_passes_through_empty_values():
    assert redact(None) is None
    assert redact("") == ""


# -- SecretResolver --------------------------------------------------------------------


class StubClient:
    """Stands in for SecretManagerServiceClient."""

    def __init__(self, values=None, error=None):
        self.values = values or {}
        self.error = error
        self.requested: list[str] = []

    def access_secret_version(self, request):
        name = request["name"]
        self.requested.append(name)
        if self.error is not None:
            raise self.error
        secret_id = name.split("/secrets/")[1].split("/versions/")[0]
        if secret_id not in self.values:
            raise KeyError(f"Secret [{secret_id}] not found")
        payload = type("Payload", (), {"data": self.values[secret_id].encode()})
        return type("Response", (), {"payload": payload})


def test_resolves_a_placeholder_into_the_secret_value():
    client = StubClient({"MTC_511_API_KEY": "abcd-1234-key"})
    resolver = SecretResolver(project="my-project", client=client)
    resolved = resolver.resolve(REAL_511_URI)
    assert resolved == "https://api.511.org/transit/vehiclepositions?api_key=abcd-1234-key&agency=EM"


def test_requests_the_secret_under_exactly_the_name_in_the_braces():
    client = StubClient({"MTC_511_API_KEY": "abcd-1234-key"})
    SecretResolver(project="my-project", client=client).resolve(REAL_511_URI)
    assert client.requested == ["projects/my-project/secrets/MTC_511_API_KEY/versions/latest"]


def test_value_is_cached_so_shared_keys_cost_one_api_call():
    """46 current feeds reference MTC_511_API_KEY; resolving each separately would be wasteful."""
    client = StubClient({"K": "abcd-1234-key"})
    resolver = SecretResolver(project="p", client=client)
    for _ in range(5):
        resolver.resolve("https://x/vp?api_key={{ K }}")
    assert len(client.requested) == 1


def test_resolved_value_is_registered_for_redaction():
    client = StubClient({"K": "abcd-1234-key"})
    resolver = SecretResolver(project="p", client=client)
    resolved = resolver.resolve("https://x/vp?whatever={{ K }}")
    assert "abcd-1234-key" in resolved
    assert "abcd-1234-key" not in redact(resolved)


def test_multiple_placeholders_in_one_url():
    client = StubClient({"USER_KEY": "user-123456", "ORG_KEY": "org-abcdef"})
    resolver = SecretResolver(project="p", client=client)
    resolved = resolver.resolve("https://x/vp?u={{ USER_KEY }}&o={{ ORG_KEY }}")
    assert resolved == "https://x/vp?u=user-123456&o=org-abcdef"


def test_missing_secret_raises_and_names_the_secret():
    resolver = SecretResolver(project="p", client=StubClient({}))
    with pytest.raises(SecretResolutionError, match="NOPE"):
        resolver.resolve("https://x/vp?api_key={{ NOPE }}")


def test_backend_failure_is_wrapped():
    resolver = SecretResolver(project="p", client=StubClient(error=PermissionError("denied")))
    with pytest.raises(SecretResolutionError, match="could not read secret"):
        resolver.resolve("https://x/vp?api_key={{ K }}")


def test_error_text_does_not_echo_a_known_secret():
    """A backend can repeat back what it was given; the wrapper must not leak it."""
    register_secret_value("leaked-secret-value")
    resolver = SecretResolver(project="p", client=StubClient(error=RuntimeError("bad token leaked-secret-value")))
    with pytest.raises(SecretResolutionError) as exc:
        resolver.resolve("https://x/vp?api_key={{ K }}")
    assert "leaked-secret-value" not in str(exc.value)


def test_empty_secret_is_an_error_rather_than_an_empty_key():
    resolver = SecretResolver(project="p", client=StubClient({"K": "   "}))
    with pytest.raises(SecretResolutionError, match="is empty"):
        resolver.resolve("https://x/vp?api_key={{ K }}")


def test_url_without_placeholders_is_returned_untouched():
    client = StubClient({})
    resolver = SecretResolver(project="p", client=client)
    url = "https://x/vp?agency=EM"
    assert resolver.resolve(url) == url
    assert client.requested == []


# -- project selection -----------------------------------------------------------------


def test_project_defaults_to_the_warehouse_project(monkeypatch):
    monkeypatch.delenv(SECRET_PROJECT_ENV_VAR, raising=False)
    assert SecretResolver(client=StubClient({})).project == DEFAULT_SECRET_PROJECT


def test_project_can_be_overridden_by_env_var(monkeypatch):
    monkeypatch.setenv(SECRET_PROJECT_ENV_VAR, "some-other-project")
    assert SecretResolver(client=StubClient({})).project == "some-other-project"


def test_explicit_project_beats_the_env_var(monkeypatch):
    monkeypatch.setenv(SECRET_PROJECT_ENV_VAR, "ignored")
    assert SecretResolver(project="explicit", client=StubClient({})).project == "explicit"
