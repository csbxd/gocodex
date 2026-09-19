#!/usr/bin/env python3
"""Maintainer-only: build Linux archives; consumers run only go build."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import tomllib

ROOT = Path(__file__).resolve().parents[1]
CONFIG = tomllib.loads((ROOT / 'Cargo.toml').read_text())
LIBRARY = CONFIG['lib']['name']
TARGETS = {'arm64': ('aarch64-unknown-linux-gnu', 'aarch64-linux-gnu.2.17'),
           'amd64': ('x86_64-unknown-linux-gnu', 'x86_64-linux-gnu.2.17')}


def run(args, **kwargs):
    print('+', shlex.join(map(str, args)), flush=True)
    subprocess.run(list(map(str, args)), cwd=ROOT, check=True, **kwargs)


def output(args, **kwargs):
    return subprocess.check_output(list(map(str, args)), cwd=ROOT, text=True, **kwargs).strip()


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build(arch, args):
    triple, zig_target = TARGETS[arch]
    target_dir = Path(os.environ.get('CARGO_TARGET_DIR', ROOT / 'target' / 'prebuild')).resolve()
    scratch = target_dir / 'packaging' / LIBRARY / arch
    scratch.mkdir(parents=True, exist_ok=True)
    zig = shutil.which(args.zig)
    if not zig:
        raise SystemExit('Install Zig 0.15.2 and set ZIG or --zig for maintainer builds')
    for tool in ['ld.lld', 'llvm-objcopy', 'llvm-ar', 'llvm-nm']:
        if not shutil.which(tool):
            raise SystemExit(f'Missing maintainer tool: {tool}')
    cc = scratch / 'cc'
    # cc-rs detects Clang and adds a Rust-style --target triple. Zig has its
    # own target grammar; retain the target (including glibc floor) selected here.
    cc.write_text("#!/usr/bin/env python3\nimport os, sys\n"
                  + "args = [a for a in sys.argv[1:] if not a.startswith('--target=')]\n"
                  + f"os.execv({zig!r}, [{zig!r}, 'cc', '-target', {zig_target!r}] + args)\n")
    cc.chmod(0o755)
    env = os.environ.copy()
    env.update({
        'CARGO_TARGET_DIR': str(target_dir),
        'CARGO_BUILD_JOBS': os.environ.get('CARGO_BUILD_JOBS', '4'),
        'CC_' + triple.replace('-', '_'): str(cc),
        'AR_' + triple.replace('-', '_'): shutil.which('llvm-ar'),
        'OPENSSL_STATIC': '1',
        'OPENSSL_NO_VENDOR': '0',
        'CFLAGS_' + triple.replace('-', '_'): '-fPIC -fno-sanitize=all',
        'ZIG_GLOBAL_CACHE_DIR': str(target_dir / 'zig-cache'),
        'RUSTFLAGS': '-C relocation-model=pic --remap-path-prefix=' + str(ROOT) + '=/source',
    })
    command = ['cargo', 'build', '--locked', '--release', '--lib', '--target', triple]
    if args.build_std:
        # Requires nightly + rust-src (or explicit RUSTC_BOOTSTRAP=1).
        command.append('-Zbuild-std=std,panic_unwind')
    if not args.package_only:
        run(command, env=env)
    original = target_dir / triple / 'release' / ('lib' + LIBRARY + '.a')
    if args.package_only:
        previous = json.loads((ROOT / 'native' / ('linux_' + arch) / 'build.json').read_text())
        if previous['cargo_lock_sha256'] != sha256(ROOT / 'Cargo.lock') or any(
            sha256(ROOT / name) != digest for name, digest in previous['bridge_source_sha256'].items()
        ):
            raise SystemExit('Compiled inputs changed; run a full prebuild')
        if previous['rustc'] != output(['rustc', '--version']) or previous['sdk_commit'] != output(['git', '-C', 'codex', 'rev-parse', 'HEAD']):
            raise SystemExit('Toolchain or SDK changed; run a full prebuild')
    exports = re.findall(r'pub (?:unsafe )?extern "C" fn (\w+)', (ROOT / 'rust/src/lib.rs').read_text())
    export_file = scratch / 'exports.txt'
    export_file.write_text('\n'.join(exports) + '\n')
    combined = scratch / 'bridge.o'
    # Resolve all internal relocations before localizing symbols. This isolates
    # Rust allocators/runtimes and vendored OpenSSL from the rest of the final Go executable.
    run(['ld.lld', '-r', '--gc-sections', *['--undefined=' + name for name in exports],
         '--whole-archive', original, '--no-whole-archive', '-o', combined])
    symbol_table = output(['llvm-nm', '--defined-only', '--format=posix', combined])
    common = [line.split()[0] for line in symbol_table.splitlines() if line.split()[1] == 'C']
    # LLVM objcopy turns a localized COMMON symbol into an absolute value!
    # Keep COMMON allocation semantics, but namespace and hide these symbols.
    private_common = [LIBRARY + '_private_' + name for name in common]
    if common:
        run(['llvm-objcopy', *['--redefine-sym=' + old + '=' + new for old, new in zip(common, private_common)], combined])
    export_file.write_text('\n'.join(exports + private_common) + '\n')
    run(['llvm-objcopy', '--keep-global-symbols=' + str(export_file),
         *['--set-symbol-visibility=' + name + '=hidden' for name in private_common], '--strip-debug', combined])
    symbols = output(['llvm-nm', '--defined-only', '--extern-only', '--format=posix', combined])
    exported = {line.split()[0] for line in symbols.splitlines()}
    if exported != set(exports + private_common):
        raise SystemExit('Unexpected exported symbols: ' + repr(exported.symmetric_difference(exports + private_common)))
    undefined = output(['llvm-nm', '--undefined-only', combined])
    if '__ubsan_' in undefined or '__asan_' in undefined:
        raise SystemExit('Archive unexpectedly requires a sanitizer runtime')
    destination = ROOT / 'native' / ('linux_' + arch)
    destination.mkdir(parents=True, exist_ok=True)
    archive = destination / ('lib' + LIBRARY + '.a')
    temp_archive = scratch / archive.name
    temp_archive.unlink(missing_ok=True)
    run(['llvm-ar', 'rcsD', temp_archive, combined])
    shutil.copyfile(temp_archive, archive.with_suffix('.a.tmp'))
    archive.with_suffix('.a.tmp').replace(archive)
    record = {
        'archive': archive.name, 'sha256': sha256(archive),
        'target': triple, 'glibc_baseline': '2.17',
        'rustc': output(['rustc', '--version']), 'zig': output([zig, 'version']),
        'sdk_commit': output(['git', '-C', 'codex', 'rev-parse', 'HEAD']),
        'cargo_lock_sha256': sha256(ROOT / 'Cargo.lock'),
        'bridge_source_sha256': {str(p.relative_to(ROOT)): sha256(p) for p in [ROOT/'build.rs', ROOT/'Cargo.toml', *sorted((ROOT/'rust/src').glob('*.rs'))]},
        'exports': sorted(exports), 'private_common_symbols': private_common, 'vendored_openssl': True,
    }
    (destination / 'build.json').write_text(json.dumps(record, indent=2) + '\n')
    print(f'Built {archive} ({archive.stat().st_size / 1024 / 1024:.1f} MiB)', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--arch', choices=[*TARGETS, 'all'], default='all')
    parser.add_argument('--zig', default=os.environ.get('ZIG', 'zig'))
    parser.add_argument('--build-std', action='store_true')
    parser.add_argument('--package-only', action='store_true', help='repackage existing Cargo output without recompiling (maintainers only)')
    args = parser.parse_args()
    for arch in TARGETS if args.arch == 'all' else [args.arch]:
        build(arch, args)


if __name__ == '__main__':
    main()
