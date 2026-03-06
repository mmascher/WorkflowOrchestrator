#!/bin/bash
set -e

# If CVMFS grid certs are mounted, merge them into the CA bundle
if [ -d /etc/grid-security/certificates ]; then
    cat /etc/grid-security/certificates/*.0 >> /etc/pki/tls/certs/ca-bundle.crt
fi

export REQUESTS_CA_BUNDLE=/etc/pki/tls/certs/ca-bundle.crt

exec "$@"
