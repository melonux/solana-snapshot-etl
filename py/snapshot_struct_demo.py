#!/usr/bin/env python3
"""Minimal Python replica of the Solana snapshot append-vec parsing used by this repo.

Supported modes:
- demo mode: synthetic in-memory snapshot for clarity
- real unpacked snapshot mode: parse an actual directory tree shaped like:
    root/
      accounts/
        439966604.13511252
        439966605.13511253
      snapshots/
        439966605/
          439966605
        status_cache
      version

The important on-disk structure is:
- each file under accounts/<slot>.<id> is an AppendVec
- each account is stored as [StoredMeta][AccountMeta][Hash][account_data]
- all fields are aligned to an 8-byte boundary
"""

from __future__ import annotations

import argparse
import re
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

ALIGN = 8
APPENDVEC_RE = re.compile(r"^(\d+)\.(\d+)$")


def align64(addr: int) -> int:
    return (addr + ALIGN - 1) & ~(ALIGN - 1)


@dataclass
class StoredMeta:
    write_version: int
    data_len: int
    pubkey: bytes

    @classmethod
    def from_bytes(cls, buf: bytes, offset: int) -> Tuple["StoredMeta", int]:
        fmt = "<QQ32s"
        size = struct.calcsize(fmt)
        raw = buf[offset : offset + size]
        if len(raw) < size:
            raise ValueError(f"StoredMeta overflow at offset {offset}")
        write_version, data_len, pubkey = struct.unpack_from(fmt, raw)
        next_offset = align64(offset + size)
        return cls(write_version, data_len, pubkey), next_offset


@dataclass
class AccountMeta:
    lamports: int
    rent_epoch: int
    owner: bytes
    executable: bool

    @classmethod
    def from_bytes(cls, buf: bytes, offset: int) -> Tuple["AccountMeta", int]:
        # Rust repr(C): u64 + u64 + [32]u8 + bool + padding to the next 8-byte boundary
        fmt = "<QQ32sB7x"
        size = struct.calcsize(fmt)
        raw = buf[offset : offset + size]
        if len(raw) < size:
            raise ValueError(f"AccountMeta overflow at offset {offset}")
        lamports, rent_epoch, owner, executable = struct.unpack_from(fmt, raw)
        next_offset = align64(offset + size)
        return cls(lamports, rent_epoch, owner, bool(executable)), next_offset


@dataclass
class StoredAccountMeta:
    meta: StoredMeta
    account_meta: AccountMeta
    data: bytes
    offset: int
    stored_size: int
    hash: bytes


class AppendVec:
    def __init__(self, payload: bytes):
        self.payload = payload

    def get_slice(self, offset: int, size: int) -> Tuple[bytes, int]:
        end = offset + size
        if end > len(self.payload):
            raise ValueError(f"slice overflow: offset={offset}, size={size}, len={len(self.payload)}")
        data = self.payload[offset:end]
        return data, align64(end)

    def get_account(self, offset: int) -> Tuple[StoredAccountMeta, int]:
        meta, next_offset = StoredMeta.from_bytes(self.payload, offset)
        account_meta, next_offset = AccountMeta.from_bytes(self.payload, next_offset)
        hash_bytes, next_offset = self.get_slice(next_offset, 32)
        data, next_offset = self.get_slice(next_offset, meta.data_len)
        stored_size = next_offset - offset
        account = StoredAccountMeta(
            meta=meta,
            account_meta=account_meta,
            data=data,
            offset=offset,
            stored_size=stored_size,
            hash=hash_bytes,
        )
        return account, next_offset

    def iter_accounts(self) -> List[StoredAccountMeta]:
        cursor = 0
        out: List[StoredAccountMeta] = []
        while cursor < len(self.payload):
            try:
                account, cursor = self.get_account(cursor)
                out.append(account)
            except ValueError:
                break
        return out


@dataclass
class SnapshotManifest:
    slot: int
    epoch: int
    block_height: int
    accounts_data_len: int
    account_count: int


def discover_snapshot_manifest(root: Path) -> Optional[Path]:
    snapshots_dir = root / "snapshots"
    if not snapshots_dir.is_dir():
        return None
    status_cache = snapshots_dir / "status_cache"
    if not status_cache.exists():
        return None
    for child in sorted(snapshots_dir.iterdir()):
        if child.is_dir():
            candidate = child / child.name
            if candidate.is_file():
                return candidate
    return None


def find_appendvec_files(root: Path) -> List[Path]:
    accounts_dir = root / "accounts"
    if not accounts_dir.is_dir():
        return []
    files = []
    for p in sorted(accounts_dir.iterdir()):
        if p.is_file() and APPENDVEC_RE.match(p.name):
            files.append(p)
    return files


def build_demo_snapshot() -> Tuple[SnapshotManifest, AppendVec]:
    manifest = SnapshotManifest(
        slot=123456,
        epoch=42,
        block_height=9000,
        accounts_data_len=256,
        account_count=2,
    )

    def make_account(pubkey: bytes, owner: bytes, lamports: int, data: bytes, write_version: int, hash_value: bytes) -> bytes:
        meta = struct.pack("<QQ32s", write_version, len(data), pubkey)
        account_meta = struct.pack("<QQ32sB7x", lamports, 7, owner, 1)
        padded_data = data + b"\x00" * ((8 - (len(data) % 8)) % 8)
        return meta + account_meta + hash_value + padded_data

    account_1 = make_account(b"\x11" * 32, b"\x22" * 32, 1_000_000, b"hello", 100, b"\xAA" * 32)
    account_2 = make_account(b"\x33" * 32, b"\x44" * 32, 2_000_000, b"world!", 101, b"\xBB" * 32)
    append_vec = AppendVec(account_1 + account_2)
    return manifest, append_vec


def print_account(account: StoredAccountMeta, index: int):
    print(f"  Account[{index}] = {{")
    print(f"    offset: {account.offset}")
    print(f"    stored_size: {account.stored_size}")
    print(f"    meta: StoredMeta(write_version={account.meta.write_version}, data_len={account.meta.data_len}, pubkey={account.meta.pubkey.hex()})")
    print(f"    account_meta: AccountMeta(lamports={account.account_meta.lamports}, rent_epoch={account.account_meta.rent_epoch}, owner={account.account_meta.owner.hex()}, executable={account.account_meta.executable})")
    print(f"    hash: {account.hash.hex()}")
    print(f"    data: {account.data!r}")
    print("  }")
    print()


def parse_real_root(root: Path):
    manifest_path = discover_snapshot_manifest(root)
    appendvecs = find_appendvec_files(root)

    print(f"Snapshot root: {root}")
    print(f"Manifest candidate: {manifest_path}")
    print(f"AppendVec files found: {len(appendvecs)}")
    if not appendvecs:
        print("No appendvec files found in accounts/. Expected names like 439966604.13511252")
        return

    for path in appendvecs:
        slot_str, vec_id = APPENDVEC_RE.match(path.name).groups()
        payload = path.read_bytes()
        vector = AppendVec(payload)
        accounts = vector.iter_accounts()
        print(f"\nAppendVec file: {path.name}  (slot={slot_str}, vec_id={vec_id})")
        print(f"Parsed account count: {len(accounts)}")
        for idx, account in enumerate(accounts, start=1):
            print_account(account, idx)


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse a Solana snapshot appendvec directory")
    parser.add_argument("--root", type=Path, default=Path("."), help="Snapshot root directory")
    parser.add_argument("--demo", action="store_true", help="Use synthetic demo data instead of a real snapshot")
    args = parser.parse_args()

    if args.demo:
        manifest, append_vec = build_demo_snapshot()
        print("SnapshotManifest")
        print(manifest)
        print()
        print("AppendVec: parsed accounts")
        for idx, account in enumerate(append_vec.iter_accounts(), start=1):
            print_account(account, idx)
        return

    parse_real_root(args.root)


if __name__ == "__main__":
    main()
