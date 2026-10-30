FROM mcr.microsoft.com/devcontainers/base:bookworm

# Install the Caltrans SSL-inspection certs before any devcontainer features
# run, so feature installs that hit the network (e.g. github.com) trust the
# CT proxy. The build context is ~/devcontainer_ssl on the host; for
# non-Caltrans users it's empty and this is a no-op.
# update-ca-certificates only picks up files ending in .crt.
COPY . /tmp/ssl_certs/
RUN for c in CTROOTCA01 CTISSUINGCA01 CTISSUINGCA02; do \
        src=/tmp/ssl_certs/$c.pem; \
        [ -f "$src" ] || src=/tmp/ssl_certs/$c.cer; \
        if [ -f "$src" ]; then sed 's/\r$//' "$src" > /usr/local/share/ca-certificates/$c.crt; fi; \
    done \
    && update-ca-certificates \
    && rm -rf /tmp/ssl_certs

    https://itsecurity.onramp.dot.ca.gov/downloads/itsecurity/files/PKI/CTROOTCA01.cer
    https://itsecurity.onramp.dot.ca.gov/downloads/itsecurity/files/PKI/CTISSUINGCA01.cer
    https://itsecurity.onramp.dot.ca.gov/downloads/itsecurity/files/PKI/CTISSUINGCA02.cer