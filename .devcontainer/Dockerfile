FROM mcr.microsoft.com/devcontainers/base:bookworm

# Install the Caltrans SSL-inspection certs before any devcontainer features
# run, so feature installs that hit the network (e.g. github.com) trust the
# CT proxy.
#
# Onramp is only reachable on the CT network and is itself served behind the
# proxy, so TLS verification is skipped (-k); each file is instead checked
# against a pinned SHA-256 and the build fails on a mismatch. Off the CT
# network the download fails and the certs are skipped, since they aren't
# needed there. This means if you are planning on developing on the CT network
# it is important to also build the container on the CT network.
# 
# update-ca-certificates only picks up files ending in .crt.

ARG CT_CERT_BASE_URL=https://itsecurity.onramp.dot.ca.gov/downloads/itsecurity/files/PKI
RUN set -e; \
    for entry in \
        CTROOTCA01=02034cb6df7767636de694cfeaeac4bcea0cac7cfe5bc42f993022702634f3e5 \
        CTISSUINGCA01=a4e3e12bcfd35b910de9789a9db2a31ffcbb73512e5d12cfdeaae3d505f36f37 \
        CTISSUINGCA02=b96306d777747ef9694dce192c05a5d9fed54e30ccfacf371afc0897bd655b50 \
    ; do \
        c=${entry%%=*}; sha=${entry#*=}; \
        if ! curl -fsSk --connect-timeout 10 --max-time 60 -o /tmp/$c.cer "$CT_CERT_BASE_URL/$c.cer"; then \
            echo "(*) Could not download $c (not on the CT network?), skipping"; \
            continue; \
        fi; \
        echo "$sha  /tmp/$c.cer" | sha256sum -c -; \ 
        sed 's/\r$//' /tmp/$c.cer > /usr/local/share/ca-certificates/$c.crt; \
        rm /tmp/$c.cer; \
    done; \
    update-ca-certificates