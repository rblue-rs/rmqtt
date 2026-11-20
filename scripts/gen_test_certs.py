#!/usr/bin/env python3
"""Generate temporary MQTT TLS test certificates in a caller-supplied directory.

Private keys and PEMs are written only under the output directory. This script
never prints certificate or key material. Requires openssl on PATH.
"""
from __future__ import annotations

import os
import subprocess
import sys


def run(cmd: list[str]) -> None:
    subprocess.check_call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)


def write_ext(path: str, body: str) -> None:
    with open(path, "w", encoding="ascii") as fh:
        fh.write(body)


def issue(
    out: str,
    name: str,
    ca_pem: str,
    ca_key: str,
    ext: str,
    not_before: str | None = None,
    not_after: str | None = None,
) -> None:
    key = os.path.join(out, f"{name}.key")
    csr = os.path.join(out, f"{name}.csr")
    pem = os.path.join(out, f"{name}.pem")
    run(
        [
            "openssl",
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            key,
            "-out",
            csr,
            "-subj",
            f"/CN={name}",
        ]
    )
    cmd = [
        "openssl",
        "x509",
        "-req",
        "-in",
        csr,
        "-CA",
        ca_pem,
        "-CAkey",
        ca_key,
        "-CAcreateserial",
        "-out",
        pem,
        "-sha256",
        "-extfile",
        ext,
    ]
    if not_before and not_after:
        cmd.extend(["-not_before", not_before, "-not_after", not_after])
    else:
        cmd.extend(["-days", "365"])
    run(cmd)
    os.remove(csr)


def make_ca(out: str, name: str) -> tuple[str, str]:
    key = os.path.join(out, f"{name}.key")
    pem = os.path.join(out, f"{name}.pem")
    run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            key,
            "-out",
            pem,
            "-days",
            "365",
            "-sha256",
            "-subj",
            f"/CN=rmqtt-{name}",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
        ]
    )
    return pem, key


def main() -> int:
    if len(sys.argv) != 2 or not sys.argv[1] or sys.argv[1].startswith("-"):
        print("usage: gen_test_certs.py OUTPUT_DIR", file=sys.stderr)
        return 2
    out = os.path.abspath(sys.argv[1])
    os.umask(0o077)
    os.makedirs(out, exist_ok=True)

    ca_pem, ca_key = make_ca(out, "ca")
    other_ca_pem, other_ca_key = make_ca(out, "other-ca")

    ip_ext = os.path.join(out, "san-ip.ext")
    dns_ext = os.path.join(out, "san-dns.ext")
    write_ext(
        ip_ext,
        "basicConstraints=CA:FALSE\n"
        "keyUsage=digitalSignature,keyEncipherment\n"
        "extendedKeyUsage=serverAuth\n"
        "subjectAltName=IP:127.0.0.1\n",
    )
    write_ext(
        dns_ext,
        "basicConstraints=CA:FALSE\n"
        "keyUsage=digitalSignature,keyEncipherment\n"
        "extendedKeyUsage=serverAuth\n"
        "subjectAltName=DNS:mqtt.example.test\n",
    )

    issue(out, "server-127", ca_pem, ca_key, ip_ext)
    issue(out, "server-wrong-host", ca_pem, ca_key, dns_ext)
    issue(
        out,
        "server-expired",
        ca_pem,
        ca_key,
        ip_ext,
        not_before="20200101000000Z",
        not_after="20200102000000Z",
    )

    client_ext = os.path.join(out, "client-auth.ext")
    write_ext(
        client_ext,
        "basicConstraints=CA:FALSE\n"
        "keyUsage=digitalSignature,keyEncipherment\n"
        "extendedKeyUsage=clientAuth\n",
    )
    issue(out, "client-ok", ca_pem, ca_key, client_ext)
    issue(out, "client-other", other_ca_pem, other_ca_key, client_ext)

    for leftover in ("ca.srl", "san-ip.ext", "san-dns.ext"):
        path = os.path.join(out, leftover)
        if os.path.exists(path):
            os.remove(path)
    # openssl -CAcreateserial may also write other-ca.srl if reused; clean any .srl/.ext
    for name in os.listdir(out):
        if name.endswith(".srl") or name.endswith(".ext") or name.endswith(".csr"):
            os.remove(os.path.join(out, name))

    print(f"wrote test certificates under {out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
