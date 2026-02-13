from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    return int(v)


def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    return float(v)


def env_str(name: str, default: str | None = None) -> str:
    v = os.getenv(name)
    if v is None:
        if default is None:
            raise RuntimeError(f"Missing env var: {name}")
        return default
    return v


@dataclass(frozen=True)
class OzonConfig:
    client_id: str
    api_key: str
    base_url: str = "https://api-seller.ozon.ru"
    rps: float = 2.0
    retry_max: int = 6
    retry_base_seconds: float = 0.7


@dataclass(frozen=True)
class MsConfig:
    token: str
    base_url: str
    rps: float = 4.0
    retry_max: int = 6
    retry_base_seconds: float = 0.6


@dataclass(frozen=True)
class FboConfig:
    dry_run: bool
    log_level: str
    poll_seconds: int

    # MS entities
    ms_org_id: str
    ms_agent_id: str
    ms_store_id: str
    ms_sales_channel_id: str
    ms_state_id: str

    # interval
    min_date_iso: str  # inclusive, e.g. 2026-02-02
    lookback_days: int

    # data paths (relative to repo root)
    data_dir: Path
    supplies_file: Path
    assortments_file: Path


def load_config(repo_root: Path) -> tuple[OzonConfig, MsConfig, FboConfig]:
    """Load config from environment. Assumes .env already loaded by the runner."""

    ozon = OzonConfig(
        client_id=env_str("OZON_CLIENT_ID"),
        api_key=env_str("OZON_API_KEY"),
        base_url=os.getenv("OZON_BASE_URL", "https://api-seller.ozon.ru"),
        rps=env_float("OZON_RPS", 2.0),
        retry_max=env_int("OZON_RETRY_MAX", 6),
        retry_base_seconds=env_float("OZON_RETRY_BASE_SECONDS", 0.7),
    )

    ms = MsConfig(
        token=env_str("MS_TOKEN"),
        base_url=env_str("MS_BASE_URL", "https://api.moysklad.ru/api/remap/1.2"),
        rps=env_float("MS_RPS", 4.0),
        retry_max=env_int("MS_RETRY_MAX", 6),
        retry_base_seconds=env_float("MS_RETRY_BASE_SECONDS", 0.6),
    )

    data_dir = repo_root / "fbo" / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    fbo = FboConfig(
        dry_run=env_bool("FBO_DRY_RUN", env_bool("DRY_RUN", False)),
        log_level=os.getenv("FBO_LOG_LEVEL", os.getenv("LOG_LEVEL", "INFO")),
        poll_seconds=env_int("POLL_SECONDS", 80),

        ms_org_id=env_str("MS_ORG_ID", "12d36dcd-8b6c-11e9-9109-f8fc00176e21"),
        ms_agent_id=env_str("MS_AGENT_ID", "f61bfcf9-2d74-11ec-0a80-04c700041e03"),
        ms_store_id=env_str("MS_FBO_STORE_ID"),
        ms_sales_channel_id=env_str("MS_SALES_CHANNEL_FBO_ID"),
        ms_state_id=env_str("MS_FBO_STATE_ID"),

        min_date_iso=os.getenv("FBO_MIN_DATE", "2026-02-02"),
        lookback_days=env_int("FBO_LOOKBACK_DAYS", 20),

        data_dir=data_dir,
        supplies_file=data_dir / "supplies.json",
        assortments_file=data_dir / "assortments.json",
    )

    return ozon, ms, fbo
