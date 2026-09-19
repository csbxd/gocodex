#!/usr/bin/env python3
"""Verify checked-in archives against their provenance records."""
import hashlib
import json
from pathlib import Path
import subprocess
import tomllib

root = Path(__file__).resolve().parents[1]
library = tomllib.loads((root/'Cargo.toml').read_text())['lib']['name']

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

for arch in ['arm64', 'amd64']:
    folder = root/'native'/('linux_'+arch)
    record = json.loads((folder/'build.json').read_text())
    archive = folder/('lib'+library+'.a')
    assert sha(archive) == record['sha256'], f'{archive}: SHA256 mismatch'
    assert sha(root/'Cargo.lock') == record['cargo_lock_sha256'], 'Cargo.lock changed; rebuild both archives'
    for name, digest in record['bridge_source_sha256'].items():
        assert sha(root/name) == digest, f'{name} changed; rebuild both archives'
    if (root/'codex'/'Cargo.toml').exists() or (root/'codex'/'.git').exists():
        sdk = subprocess.check_output(['git','-C',str(root/'codex'),'rev-parse','HEAD'],text=True).strip()
        assert sdk == record['sdk_commit'], 'Codex revision changed; rebuild both archives'
    print(f'{arch}: {archive.name} SHA256 verified')
