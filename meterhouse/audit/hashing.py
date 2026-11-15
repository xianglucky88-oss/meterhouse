"""规范化编码与完整性哈希。

审计交付物必须可重放：同一组数据在任何机器、任何 Python 进程下产出的
字节序列与哈希完全一致。因此这里不依赖 ``json.dumps`` 的默认行为，
而是固定键序、分隔符、ensure_ascii 与时间戳表示。
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

# 交付物中时间戳统一为 ISO-8601 UTC 字符串（与 DuckDB TIMESTAMP 的取值一致）
CANONICAL_SEPARATORS = (",", ":")


def canonical_value(value: Any) -> Any:
    """把 DuckDB 返回值转换为规范化的 JSON 可序列化值。"""
    if isinstance(value, datetime):
        # DuckDB TIMESTAMP 为 naive UTC，统一输出不带时区的 ISO 文本
        if value.tzinfo is not None:
            value = value.astimezone(tz=None).replace(tzinfo=None)
        return value.isoformat(sep="T")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    return value


def canonical_json(row: dict[str, Any]) -> bytes:
    """单行的规范化 JSON 字节串。

    键按列名字典序排列，无空白、不转义非 ASCII，行尾追加 ``\\n``
    （JSON Lines 交付格式）。
    """
    ordered = {k: canonical_value(row[k]) for k in sorted(row)}
    payload = json.dumps(
        ordered,
        ensure_ascii=False,
        separators=CANONICAL_SEPARATORS,
        allow_nan=False,
        sort_keys=True,
    )
    return (payload + "\n").encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class RowHasher:
    """增量计算行哈希与段哈希链。

    段哈希定义：

        segment_hash = SHA256( prev_segment_hash || row_json || row_json || ... )

    其中 prev_segment_hash 为同数据集上一段的段哈希，首段使用固定创世值。
    哈希链让任一行的增删改或段顺序调换都会导致最终根哈希变化，
    审计方可逐段重算验证「不漏不重」。
    """

    GENESIS = "0" * 64

    def __init__(self, previous_hash: str = GENESIS):
        self._chain = hashlib.sha256()
        self._chain.update(previous_hash.encode("ascii"))
        self._row_count = 0

    def add_row(self, row: dict[str, Any]) -> bytes:
        encoded = canonical_json(row)
        self._chain.update(encoded)
        self._row_count += 1
        return encoded

    def update(self, data: bytes) -> None:
        """喂入已编码字节，供审计方按段文件原样重放哈希链。"""
        self._chain.update(data)

    def digest(self) -> str:
        return self._chain.hexdigest()

    @property
    def row_count(self) -> int:
        return self._row_count


def file_sha256(path: str, chunk_size: int = 1024 * 1024) -> str:
    """分块计算文件 SHA-256，不把整个文件读入内存。"""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()
