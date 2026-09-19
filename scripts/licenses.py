#!/usr/bin/env python3
"""Collect license notices for the statically distributed Rust dependencies."""
import json
import os
from pathlib import Path
import subprocess

root=Path(__file__).resolve().parents[1]
metadata=json.loads(subprocess.check_output(['cargo','metadata','--locked','--offline','--format-version','1','--filter-platform','aarch64-unknown-linux-gnu'],cwd=root))
parts=['Third-party notices for the bundled native archives.\nThe bridge embeds the following Rust crates and their native dependencies.\nPackage licenses and bundled notices are reproduced below.\n']
for package in sorted(metadata['packages'],key=lambda p:(p['name'],p['version'])):
    if package['name'].startswith('gocodex-'):continue
    directory=Path(package['manifest_path']).parent
    parts.append('\n'+'='*72+'\n'+package['name']+' '+package['version']+'\nLicense expression: '+str(package.get('license'))+'\nRepository: '+str(package.get('repository'))+'\n')
    notices=[]
    for current,dirs,files in os.walk(directory):
        dirs[:]=[d for d in dirs if d not in ['.git','target','test','tests']]
        for name in files:
            upper=name.upper()
            if upper.startswith(('LICENSE','LICENCE','COPYING','NOTICE')) and not upper.endswith(('.RS','.C','.H','.PY')):
                p=Path(current)/name
                if p.is_file() and p.stat().st_size<2_000_000:notices.append(p)
    for notice in sorted(notices):
        parts.append('\n--- '+str(notice.relative_to(directory))+' ---\n'+notice.read_text(errors='replace')+'\n')
for name in ['LICENSE','NOTICE']:
    parts.append('\n--- Codex '+name+' ---\n'+(root/'codex'/name).read_text())
# The Rust distribution contains the complete standard-library attribution set.
sysroot=Path(subprocess.check_output(['rustc','--print','sysroot'],text=True).strip())
for name in ['LICENSE-MIT','LICENSE-APACHE','COPYRIGHT-library.html']:
    source=sysroot/'share/doc/rust'/name
    if source.exists():parts.append('\n--- Rust standard library '+name+' ---\n'+source.read_text())
(root/'THIRD_PARTY_NOTICES.txt').write_text('\n'.join(line.rstrip() for line in ''.join(parts).splitlines())+'\n')
print('Updated THIRD_PARTY_NOTICES.txt')
