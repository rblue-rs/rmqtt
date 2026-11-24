#!/usr/bin/env python3
"""Synchronize the single plugin's bundled native source (never dependencies/binaries)."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
DEST = ROOT / 'examples/flutter/mqtt_flutter/native'


def inputs():
    names = ['Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml', 'LICENSE',
             'bindings/c/include/mqtt_client.h']
    for crate in sorted((ROOT / 'crates').iterdir()):
        if not crate.is_dir():
            continue
        for name in ('Cargo.toml', 'build.rs', 'cbindgen.toml', 'LICENSE', 'README.md'):
            if (crate / name).is_file():
                names.append(str((crate / name).relative_to(ROOT)))
        for dirname in ('src', 'third_party'):
            for path in sorted((crate / dirname).rglob('*')):
                if path.is_file():
                    names.append(str(path.relative_to(ROOT)))
    return {name: (ROOT / name).read_bytes() for name in sorted(names)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    files = inputs()
    files['SOURCES.json'] = (json.dumps({name: hashlib.sha256(data).hexdigest()
                                       for name, data in files.items()},
                                      indent=2, sort_keys=True) + '\n').encode()
    # Do not walk, read, delete or package local Cargo build output.
    actual = {str(p.relative_to(DEST)) for p in DEST.rglob('*')
              if p.is_file() and 'target' not in p.relative_to(DEST).parts}
    if extra := actual - files.keys():
        print('Unexpected native source files (inspect/remove manually): ' + str(sorted(extra)),
              file=sys.stderr)
        return 1
    stale = []
    for name, data in files.items():
        path = DEST / name
        if path.is_symlink():
            raise ValueError('Native source must not be a symlink: ' + name)
        if not path.is_file() or path.read_bytes() != data:
            stale.append(name)
            if not args.check:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(data)
    if args.check and stale:
        print('Bundled source stale: ' + ', '.join(stale), file=sys.stderr)
        return 1
    print(f'Bundled source {"checked" if args.check else "synchronized"}: {len(files)} files')
    return 0


if __name__ == '__main__':
    sys.exit(main())
