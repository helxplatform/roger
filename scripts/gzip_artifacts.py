#!/usr/bin/env python
"""Gzip-compress annotate/crawl artifacts committed before storage.py's
write_object started gzipping them (see roger.core.storage).

A dataset annotated before that fix has its elements.txt/concepts.txt sitting
in lakefs uncompressed -- ~3MB/dir raw for a typical dbGaP data dict pair.
crawl's own output is gzipped going forward, but crawl still has to
*download* that old, uncompressed annotation output as input first: for
bdc-parent's 61,597 dirs that is ~186GB of local disk before crawl writes a
single byte, which is most of what blew the crawl task's 200G PVC.

Unlike migrate_pickled_classes.py this does no jsonpickle decode/re-encode --
pure byte-level gzip, so the serialized content is untouched, just smaller.

    lakectl local clone lakefs://<repo>/<branch>/annotate_and_index/<task>/ ./out
    python scripts/gzip_artifacts.py ./out            # compress in place
    lakectl local commit ./out -m "gzip annotation output"

    python scripts/gzip_artifacts.py --dry-run ./out  # report only
    python scripts/gzip_artifacts.py --self-check     # no dir needed
"""

import argparse
import gzip
import sys
from pathlib import Path

ARTIFACTS = ('elements.txt', 'concepts.txt', 'expanded_concepts.txt')
GZIP_MAGIC = b'\x1f\x8b'
# level 6 matches storage.py's write_object -- level 9 (gzip's default)
# burned 3-4x the cpu for the same ratio on this repetitive JSON.
COMPRESSLEVEL = 6


def artifact_files(root):
    return sorted(p for p in Path(root).rglob('*.txt') if p.name in ARTIFACTS)


def compress_file(path, dry_run=False):
    """Returns (raw_size, compressed_size) if compressed, None if skipped
    (already gzip or empty)."""
    raw = path.read_bytes()
    if not raw or raw[:2] == GZIP_MAGIC:
        return None
    compressed = gzip.compress(raw, compresslevel=COMPRESSLEVEL)
    if not dry_run:
        tmp = path.with_name(path.name + '.gzip-tmp')
        tmp.write_bytes(compressed)
        tmp.replace(path)
    return len(raw), len(compressed)


def run(root, dry_run=False):
    files = artifact_files(root)
    total_raw = total_compressed = 0
    changed = skipped = 0
    for i, path in enumerate(files, 1):
        result = compress_file(path, dry_run=dry_run)
        if result is None:
            skipped += 1
            continue
        raw_size, compressed_size = result
        total_raw += raw_size
        total_compressed += compressed_size
        changed += 1
        if changed % 5000 == 0:
            print(f"  {changed} compressed ({i}/{len(files)} scanned)")
    verb = "would compress" if dry_run else "compressed"
    print(f"{verb} {changed} of {len(files)} file(s), "
          f"{skipped} already gzip or empty")
    if total_raw:
        print(f"{total_raw / 1e9:.2f}GB -> {total_compressed / 1e9:.2f}GB "
              f"({total_raw / total_compressed:.1f}x)")
    return changed


def self_check():
    "Round-trip a small payload; fails loudly if compress_file regresses."
    import tempfile
    payload = b'{"id": "UMLS:C1"}' * 500

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / 'concepts.txt'
        path.write_bytes(payload)

        result = compress_file(path, dry_run=True)
        assert result is not None, "dry-run should report a would-be change"
        assert path.read_bytes() == payload, "dry-run must not touch the file"

        result = compress_file(path)
        assert result is not None
        raw_size, compressed_size = result
        assert raw_size == len(payload)
        assert compressed_size < raw_size, "should have actually shrunk"

        with open(path, 'rb') as f:
            assert f.read(2) == GZIP_MAGIC
        assert gzip.decompress(path.read_bytes()) == payload

        # idempotent: running again on an already-gzipped file is a no-op
        assert compress_file(path) is None
        assert gzip.decompress(path.read_bytes()) == payload

    print("self-check ok")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('root', nargs='?', help='directory to compress in place')
    parser.add_argument('--dry-run', action='store_true',
                        help='report what would change without writing')
    parser.add_argument('--self-check', action='store_true',
                        help='round-trip a synthetic payload, no dir needed')
    args = parser.parse_args()

    if args.self_check:
        self_check()
        return
    if not args.root:
        parser.error("root directory required unless --self-check")
    run(args.root, dry_run=args.dry_run)


if __name__ == '__main__':
    sys.exit(main())
