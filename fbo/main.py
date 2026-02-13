from __future__ import annotations

import logging
import os
import time
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:  # optional
    load_dotenv = None

from .config import load_config
from .http_client import JsonHttpClient, RetryPolicy
from .logging_utils import setup_logger, log
from .ms_api import MsApi
from .ozon_api import OzonApi
from .storage import load_json, save_json
from .sync import sync_once


def repo_root_from_here() -> Path:
    # fbo/main.py -> fbo -> repo root
    return Path(__file__).resolve().parents[1]


def main() -> int:
    repo_root = repo_root_from_here()

    # Load .env from repo root (ms-ozon-sync/.env)
    if load_dotenv is not None:
        env_path = repo_root / ".env"
        if env_path.exists():
            load_dotenv(env_path)

    ozon_cfg, ms_cfg, fbo_cfg = load_config(repo_root)

    logger = setup_logger(
        name="fbo",
        level=fbo_cfg.log_level,
        log_file=(fbo_cfg.data_dir / "sync_fbo.log"),
    )

    log(logger, logging.INFO, "start", op="start", entity="fbo", err=None)
    log(logger, logging.INFO, f"dry_run={fbo_cfg.dry_run}", op="config")

    ozon_client = JsonHttpClient(
        base_url=ozon_cfg.base_url,
        headers={
            "Client-Id": ozon_cfg.client_id,
            "Api-Key": ozon_cfg.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        rps=ozon_cfg.rps,
        retry=RetryPolicy(max_attempts=ozon_cfg.retry_max, base_sleep=ozon_cfg.retry_base_seconds),
    )

    ms_client = JsonHttpClient(
        base_url=ms_cfg.base_url,
        headers={
            "Authorization": f"Bearer {ms_cfg.token}",
            "Accept": "application/json;charset=utf-8",
            "Content-Type": "application/json",
        },
        rps=ms_cfg.rps,
        retry=RetryPolicy(max_attempts=ms_cfg.retry_max, base_sleep=ms_cfg.retry_base_seconds),
    )

    ozon = OzonApi(ozon_client)
    ms = MsApi(ms_client)

    # memory
    supplies = load_json(fbo_cfg.supplies_file)
    assortments = load_json(fbo_cfg.assortments_file)

    while True:
        try:
            created, skipped = sync_once(logger, fbo_cfg, ozon, ms, supplies, assortments)
            log(logger, logging.INFO, f"cycle_done created={created} skipped={skipped}", op="cycle")
            save_json(fbo_cfg.supplies_file, supplies)
            save_json(fbo_cfg.assortments_file, assortments)
        except KeyboardInterrupt:
            log(logger, logging.INFO, "stop", op="stop")
            save_json(fbo_cfg.supplies_file, supplies)
            save_json(fbo_cfg.assortments_file, assortments)
            return 0
        except Exception as e:
            log(logger, logging.ERROR, "cycle_error", op="error", err=str(e))
            # persist what we have to avoid losing progress
            try:
                save_json(fbo_cfg.supplies_file, supplies)
                save_json(fbo_cfg.assortments_file, assortments)
            except Exception:
                pass

        time.sleep(fbo_cfg.poll_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
