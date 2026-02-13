from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from .config import FboConfig
from .logging_utils import log
from .ms_api import MsApi, meta
from .ozon_api import OzonApi
from .http_client import HttpError


ACTIVE_STATES = [
    "READY_TO_SUPPLY",
    "ACCEPTED_AT_SUPPLY_WAREHOUSE",
    "IN_TRANSIT",
    "ACCEPTANCE_AT_STORAGE_WAREHOUSE",
    "REPORTS_CONFIRMATION_AWAITING",
    "COMPLETED",
    "OVERDUE",
]

CANCELLED_STATES = {
    "CANCELLED",
    "REJECTED_AT_SUPPLY_WAREHOUSE",
    "REPORT_REJECTED",
}


def is_ms_name_conflict(err: Exception) -> bool:
    if not isinstance(err, HttpError):
        return False
    body = (err.body or "").lower()
    # common MS messages: "уже существует", "unique", "name"
    return ("уже существует" in body) or ("unique" in body) or ("name" in body and "дубли" in body)


def is_ms_stock_error(err: Exception) -> bool:
    if not isinstance(err, HttpError):
        return False
    body = (err.body or "").lower()
    # Typical MS stock issues: "недостаточно", "остат", "на складе", "stock"
    needles = ["недостат", "остат", "на складе", "stock", "not enough"]
    return any(n in body for n in needles)


def get_timeslot_from_ozon_order(order: Dict[str, Any]) -> Optional[str]:
    # /v3/supply-order/get shape
    ts = order.get("timeslot") or {}
    return ((ts.get("timeslot") or {}).get("from"))


def get_supply_first(order: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    sups = order.get("supplies") or []
    return sups[0] if sups else None


def normalize_offer_id(s: str) -> str:
    t = (s or "").strip().upper()
    # Cyrillic->Latin for common lookalikes
    table = {
        "А": "A", "В": "B", "Е": "E", "К": "K", "М": "M", "Н": "H", "О": "O", "Р": "P", "С": "C", "Т": "T", "Х": "X",
    }
    for k, v in table.items():
        t = t.replace(k, v)
    t = t.replace("–", "-").replace("—", "-")
    t = "-".join([p.strip() for p in t.split("-")])
    return t


def iso_to_ms_moment(iso_z: str) -> str:
    # Ozon gives ISO Z; MS accepts "YYYY-MM-DD HH:MM:SS"
    dt = datetime.fromisoformat(iso_z.replace("Z", "+00:00")).astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def ms_default_sale_price(ms_item: Dict[str, Any]) -> Optional[int]:
    prices = ms_item.get("salePrices") or []
    # prefer exact name "Цена продажи"
    for p in prices:
        pt = (p.get("priceType") or {}).get("name")
        if pt == "Цена продажи":
            try:
                return int(round(float(p.get("value")), 0))
            except Exception:
                return None
    if prices:
        try:
            return int(round(float(prices[0].get("value")), 0))
        except Exception:
            return None
    return None


def resolve_assortment(
    logger: logging.Logger,
    ms: MsApi,
    cache: Dict[str, Any],
    offer_id: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Return cached record for offer_id or build it.

    Record schema:
      {kind:'product', meta:{href,type,mediaType}, price:int}
      {kind:'bundle', components:[{meta, qty, price}], meta:bundleMeta}
    """
    key = normalize_offer_id(offer_id)
    if key in cache:
        return cache[key], None

    # product?
    row = ms.search_product_by_article(key)
    if row:
        full = ms.get_product(row["id"])
        price = ms_default_sale_price(full)
        if price is None:
            return None, f"MS product {key} has no salePrices"
        rec = {
            "kind": "product",
            "meta": {"href": full["meta"]["href"], "type": "product", "mediaType": "application/json"},
            "price": price,
            "article": key,
        }
        cache[key] = rec
        return rec, None

    # bundle?
    brow = ms.search_bundle_by_article(key)
    if brow:
        bfull = ms.get_bundle(brow["id"])
        comps = ms.get_bundle_components(brow["id"]).get("rows") or []
        if not comps:
            return None, f"MS bundle {key} has no components"

        comp_recs: List[Dict[str, Any]] = []
        for c in comps:
            a = (c.get("assortment") or {}).get("meta") or {}
            href = a.get("href")
            typ = a.get("type")
            qty = c.get("quantity")
            if not href or not typ:
                return None, f"MS bundle {key} component missing meta"
            # fetch component to get default sale price
            # href is full URL; ms client works with path; easiest: parse id
            comp_id = href.rstrip("/").split("/")[-1]
            if typ == "product":
                comp_full = ms.get_product(comp_id)
            elif typ == "variant":
                # variant prices are on variant entity
                comp_full = ms.c.request("GET", f"/entity/variant/{comp_id}")
            else:
                return None, f"MS bundle {key} component type not supported: {typ}"

            price = ms_default_sale_price(comp_full)
            if price is None:
                return None, f"MS component {comp_id} has no salePrices"

            try:
                qn = int(round(float(qty), 0))
            except Exception:
                qn = int(qty) if qty is not None else 1

            comp_recs.append({
                "meta": {"href": href, "type": typ, "mediaType": "application/json"},
                "qty": qn,
                "price": price,
            })

        rec = {
            "kind": "bundle",
            "meta": {"href": bfull["meta"]["href"], "type": "bundle", "mediaType": "application/json"},
            "components": comp_recs,
            "article": key,
        }
        cache[key] = rec
        return rec, None

    return None, f"MS assortment not found by article={key}"


def sync_once(
    logger: logging.Logger,
    cfg: FboConfig,
    ozon: OzonApi,
    ms: MsApi,
    supplies_mem: Dict[str, Any],
    assort_cache: Dict[str, Any],
) -> Tuple[int, int]:
    """Returns (created_count, skipped_count)."""

    # We list order_ids, then fetch orders in batches via /v3/supply-order/get (includes warehouse name/address)
    # and filter by timeslot.timeslot.from.
    # Window start = max(today-lookback_days, cfg.min_date_iso) (inclusive), based on TIMESLOT_FROM_UTC.
    today = date.today()
    start_date = max(date.fromisoformat(cfg.min_date_iso), today - timedelta(days=cfg.lookback_days))
    start_dt = datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc)

    log(
        logger,
        logging.INFO,
        "ozon.list_supply_orders",
        op="ozon.list",
        start_date=start_date.isoformat(),
        sort_by="TIMESLOT_FROM_UTC",
        sort_dir="DESC",
        err=None,
    )

    last_id: Optional[str] = None
    order_ids: List[int] = []

    while True:
        resp = ozon.list_supply_orders(
            states=ACTIVE_STATES + sorted(list(CANCELLED_STATES)),
            limit=100,
            last_id=last_id,
            sort_by="TIMESLOT_FROM_UTC",
            sort_dir="DESC",
        )
        ids = resp.get("order_ids") or []
        order_ids.extend([int(x) for x in ids])
        last_id = resp.get("last_id")
        if not last_id or not ids:
            break

    created = 0
    skipped = 0

    def _is_name_conflict(err: HttpError) -> bool:
        b = (err.body or "").lower()
        return err.status in (409, 412, 422) or ("name" in b and ("уже" in b or "exists" in b or "unique" in b))

    def _is_stock_error(err: HttpError) -> bool:
        b = (err.body or "").lower()
        return any(x in b for x in ["остат", "stock", "not enough", "недостат", "available", "quantity"])

    # Batch get order details (with warehouse names)
    batch_size = 50
    for i in range(0, len(order_ids), batch_size):
        batch = order_ids[i : i + batch_size]
        log(logger, logging.INFO, "ozon.get", op="ozon.get", count=len(batch))
        got = ozon.get_supply_orders(batch)
        orders = got.get("orders") or []

        for det in orders:
            oid = det.get("order_id")
            if oid is None:
                skipped += 1
                continue
            order_id = str(oid)
            order_number = str(det.get("order_number") or "")
            state = str(det.get("state") or "")

            if not order_number:
                log(logger, logging.WARNING, "skip.missing_order_number", op="skip", order_id=order_id)
                skipped += 1
                continue

            if state in CANCELLED_STATES:
                supplies_mem.pop(order_id, None)
                log(logger, logging.INFO, "skip.cancelled", op="skip", order_id=order_id, order_number=order_number, state=state)
                skipped += 1
                continue

            # timeslot filter (business rule)
            timeslot_from = None
            try:
                timeslot_from = ((det.get("timeslot") or {}).get("timeslot") or {}).get("from")
            except Exception:
                timeslot_from = None

            if not timeslot_from:
                log(logger, logging.INFO, "skip.no_timeslot", op="skip", order_id=order_id, order_number=order_number, state=state)
                skipped += 1
                continue

            try:
                ts_dt = datetime.fromisoformat(timeslot_from.replace("Z", "+00:00")).astimezone(timezone.utc)
            except Exception:
                log(logger, logging.WARNING, "skip.bad_timeslot", op="skip", order_id=order_id, order_number=order_number, timeslot_from=timeslot_from)
                skipped += 1
                continue

            if ts_dt < start_dt:
                log(
                    logger,
                    logging.INFO,
                    "skip.timeslot_before_window",
                    op="skip",
                    order_id=order_id,
                    order_number=order_number,
                    state=state,
                    timeslot_from=timeslot_from,
                    window_from=start_dt.isoformat(),
                )
                skipped += 1
                continue

            # memory short-circuit: if seen and state unchanged and READY_TO_SUPPLY, skip but keep
            mem = supplies_mem.get(order_id)
            if mem and mem.get("state") == state and state == "READY_TO_SUPPLY":
                log(logger, logging.INFO, "skip.same_state_ready", op="skip", order_id=order_id, order_number=order_number, state=state)
                skipped += 1
                continue

            # Ensure MS order exists (create if missing, but also proceed if already exists because we must create Move)
            existing = ms.find_customerorder_by_name(order_number)

            # use timeslot.from for deliveryPlannedMoment
            delivery_planned = timeslot_from

            # destination warehouse descriptor (name preferred)
            supplies = det.get("supplies") or []
            storage_name = None
            dest_id = None
            bundle_id = None
            if supplies:
                s0 = supplies[0]
                sw = (s0.get("storage_warehouse") or {})
                dest_id = sw.get("warehouse_id")
                storage_name = sw.get("name")
                bundle_id = s0.get("bundle_id") or ((s0.get("content") or {}).get("bundle_id"))

            dest_desc = storage_name or (str(dest_id) if dest_id is not None else "")
            comment = f"{order_number} - {dest_desc}".strip(" -")

            if not bundle_id:
                log(logger, logging.ERROR, "ozon.bundle_id_missing", op="error", order_id=order_id, order_number=order_number, err="bundle_id missing")
                skipped += 1
                continue

            # Pull bundle items (Ozon)
            log(logger, logging.INFO, "ozon.bundle_items", op="ozon.bundle", order_id=order_id, order_number=order_number)
            oz_items = ozon.bundle_items_all(bundle_id)

            # Build MS positions (expand bundles)
            positions: List[Dict[str, Any]] = []
            errors: List[str] = []

            for oi in oz_items:
                offer_id = str(oi.get("offer_id") or "")
                qty_raw = oi.get("quantity")
                if not offer_id:
                    continue
                try:
                    qty = int(qty_raw)
                except Exception:
                    qty = int(round(float(qty_raw), 0)) if qty_raw is not None else 1

                rec, err = resolve_assortment(logger, ms, assort_cache, offer_id)
                if err:
                    errors.append(f"{offer_id}: {err}")
                    continue

                if rec["kind"] == "product":
                    positions.append({
                        "quantity": qty,
                        "price": rec["price"],
                        "assortment": {"meta": rec["meta"]},
                    })
                elif rec["kind"] == "bundle":
                    for c in rec["components"]:
                        positions.append({
                            "quantity": qty * int(c["qty"]),
                            "price": int(c["price"]),
                            "assortment": {"meta": c["meta"]},
                        })
                else:
                    errors.append(f"{offer_id}: unknown kind")

            if errors:
                log(logger, logging.ERROR, "ms.assortment_resolve_failed", op="error", order_id=order_id, order_number=order_number, err="; ".join(errors)[:1500])
                skipped += 1
                continue

            co: Optional[Dict[str, Any]] = None
            if existing:
                co = existing
                log(logger, logging.INFO, "ms.customerorder_exists", op="ms.exists", order_id=order_id, order_number=order_number, ms_id=existing.get("id"))
            else:
                # Create CustomerOrder
                body = {
                    "name": order_number,
                    "description": comment,
                    "organization": meta(f"{ms.c.base_url}/entity/organization/{cfg.ms_org_id}", "organization"),
                    "agent": meta(f"{ms.c.base_url}/entity/counterparty/{cfg.ms_agent_id}", "counterparty"),
                    "store": meta(f"{ms.c.base_url}/entity/store/{cfg.ms_store_id}", "store"),
                    "salesChannel": meta(f"{ms.c.base_url}/entity/saleschannel/{cfg.ms_sales_channel_id}", "saleschannel"),
                    "state": meta(f"{ms.c.base_url}/entity/customerorder/metadata/states/{cfg.ms_state_id}", "state"),
                    "positions": positions,
                }
                if delivery_planned:
                    body["deliveryPlannedMoment"] = iso_to_ms_moment(delivery_planned)

                if cfg.dry_run:
                    log(logger, logging.INFO, "DRY_RUN.ms.create_customerorder", op="dry", order_id=order_id, order_number=order_number)
                    # still proceed to dry-run move creation
                    co = {"id": "DRY", "meta": {"href": "DRY"}}
                else:
                    log(logger, logging.INFO, "ms.create_customerorder", op="ms.create", order_id=order_id, order_number=order_number)
                    co = ms.create_customerorder(body)
                    created += 1

            # Create Move linked to CustomerOrder
            if co is None:
                skipped += 1
                continue

            # If Move already exists -> forget supply
            mv_existing = ms.find_move_by_name(order_number)
            if mv_existing:
                log(logger, logging.INFO, "ms.move_exists_forget", op="ms.exists", order_id=order_id, order_number=order_number, ms_move_id=mv_existing.get("id"))
                supplies_mem.pop(order_id, None)
                skipped += 1
                continue

            move_body = {
                "name": order_number,
                "description": comment,
                "organization": meta(f"{ms.c.base_url}/entity/organization/{cfg.ms_org_id}", "organization"),
                "agent": meta(f"{ms.c.base_url}/entity/counterparty/{cfg.ms_agent_id}", "counterparty"),
                "sourceStore": meta(f"{ms.c.base_url}/entity/store/{cfg.ms_move_source_store_id}", "store"),
                "targetStore": meta(f"{ms.c.base_url}/entity/store/{cfg.ms_move_target_store_id}", "store"),
                "state": meta(f"{ms.c.base_url}/entity/move/metadata/states/{cfg.ms_move_state_id}", "state"),
                "customerOrder": meta(f"{ms.c.base_url}/entity/customerorder/{co.get('id')}", "customerorder"),
                "positions": positions,
                "applicable": True,
            }

            if cfg.dry_run:
                log(logger, logging.INFO, "DRY_RUN.ms.create_move", op="dry", order_id=order_id, order_number=order_number)
                supplies_mem[order_id] = {
                    "order_number": order_number,
                    "state": state,
                    "ms": {"dry": True},
                    "move": {"dry": True},
                }
                skipped += 1
                continue

            try:
                log(logger, logging.INFO, "ms.create_move", op="ms.create", order_id=order_id, order_number=order_number, applicable=True)
                mv = ms.create_move(move_body)
            except HttpError as e:
                if _is_name_conflict(e):
                    log(logger, logging.ERROR, "ms.create_move_name_conflict_forget", op="error", order_id=order_id, order_number=order_number, err=str(e))
                    supplies_mem.pop(order_id, None)
                    skipped += 1
                    continue
                if _is_stock_error(e):
                    move_body["applicable"] = False
                    log(logger, logging.WARNING, "ms.create_move_retry_not_applicable", op="ms.retry", order_id=order_id, order_number=order_number)
                    mv = ms.create_move(move_body)
                else:
                    raise

            supplies_mem[order_id] = {
                "order_number": order_number,
                "state": state,
                "ms": {"id": co.get("id"), "href": (co.get("meta") or {}).get("href")},
                "move": {"id": mv.get("id"), "href": (mv.get("meta") or {}).get("href"), "applicable": mv.get("applicable")},
            }

    return created, skipped
