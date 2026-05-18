"""Google ADC auth concerns: pre-flight check and the bundled login config wrapper.

This package also bundles `login.json`, the Cal-ITP gcloud login config consumed by
`do_login()` and surfaced via the CLI's `login` command.
"""

import subprocess
from importlib.resources import files

LOGIN_CONFIG = str(files("calitp_portfolio.auth") / "login.json")


def is_valid() -> bool:
    """True iff the operator has valid Google credentials available to the tool."""
    result = subprocess.run(
        ["gcloud", "auth", "application-default", "print-access-token"],
        capture_output=True,
    )
    return result.returncode == 0


def login() -> int:
    """Run `gcloud auth application-default login` with the bundled Cal-ITP config. Returns gcloud's exit code."""
    cmd = ["gcloud", "auth", "application-default", "login", f"--login-config={LOGIN_CONFIG}"]
    return subprocess.run(cmd).returncode
