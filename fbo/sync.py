from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from .config import FboConfig
from .logging_utils import log
from .ms_api import MsApi, meta
from .ozon_api import OzonApi


ACTIVE_STATES = [
    "ORDER_STATE_READY_TO_SUPPLY",
    "ORDER_STATE_ACCEPTED_AT_SUPPLY_WAREHOUSE",
    "ORDER_STATE_IN_TRANSIT",
    "ORDER_STATE_ACCEPTANCE_AT_STORAGE_WAREHOUSE",
    "ORDER_STATE_REPORTS_CONFIRMATION_AWAITING",
    "ORDER_STATE_COMPLETED",
    "ORDER_STATE_OVERDUE",
]

CANCELLED_STATES = {
    "ORDER_STATE_CANCELLED",
    "ORDER_STATE_REJECTED_AT_SUPPLY_WAREHOUSE",
    "ORDER_STATE_REPORT_REJECTED",
}


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

    today = date.today()
    start = max(date.fromisoformat(cfg.min_date_iso), today - timedelta(days=cfg.lookback_days))
    created_from = start.isoformat() + "T00:00:00Z"

    log(logger, logging.INFO, "ozon.list_supply_orders", op="ozon.list", err=None)

    last_id: Optional[str] = None
    to_process: List[Dict[str, Any]] = []

    while True:
        resp = ozon.list_supply_orders(states=ACTIVE_STATES + list(CANCELLED_STATES), limit=100, last_id=last_id, created_from=created_from)
        items = resp.get("items") or resp.get("orders") or []
        to_process.extend(items)
        last_id = resp.get("last_id")
        has_next = bool(resp.get("has_next"))
        if not has_next:
            break
        if not last_id:
            break

    created = 0
    skipped = 0

    for it in to_process:
        order_id = str(it.get("order_id") or it.get("id") or "")
        order_number = str(it.get("order_number") or it.get("orderNumber") or "")
        state = str(it.get("state") or it.get("order_state") or "")

        if not order_id or not order_number:
            log(logger, logging.WARNING, "skip.item_missing_ids", op="skip", order_id=order_id, order_number=order_number)
            skipped += 1
            continue

        if state in CANCELLED_STATES:
            # we ignore cancelled; also forget if existed
            if order_id in supplies_mem:
                supplies_mem.pop(order_id, None)
            log(logger, logging.INFO, "skip.cancelled", op="skip", order_id=order_id, order_number=order_number, entity="ozon")
            skipped += 1
            continue

        # memory short-circuit: if seen and state unchanged and state == READY_TO_SUPPLY, skip but keep
        mem = supplies_mem.get(order_id)
        if mem and mem.get("state") == state and state == "ORDER_STATE_READY_TO_SUPPLY":
            log(logger, logging.INFO, "skip.same_state_ready", op="skip", order_id=order_id, order_number=order_number, entity="ozon")
            skipped += 1
            continue

        # if not in memory, or state changed, we must ensure MS order exists; if exists -> skip & forget
        existing = ms.find_customerorder_by_name(order_number)
        if existing:
            log(logger, logging.INFO, "ms.customerorder_exists_skip_forget", op="ms.exists", order_id=order_id, order_number=order_number, ms_id=existing.get("id"))
            supplies_mem.pop(order_id, None)
            skipped += 1
            continue

        # Need full details to build comment and bundle_id/timeslot
        log(logger, logging.INFO, "ozon.details", op="ozon.details", order_id=order_id, order_number=order_number)
        det = ozon.details(int(order_id))

        delivery_planned = None
        try:
            delivery_planned = (((det.get("timeslot") or {}).get("value") or {}).get("timeslot") or {}).get("from")
        except Exception:
            delivery_planned = None

        # destination warehouse descriptor
        supplies = det.get("supplies") or []
        dest = None
        bundle_id = None
        if supplies:
            s0 = supplies[0]
            dest = ((s0.get("storage_warehouse") or {}).get("warehouse_id"))
            bundle_id = (((s0.get("content") or {}).get("bundle_id")))

        comment = f"{order_number} - {dest if dest is not None else ''}".strip(" -")

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
            created += 1
            supplies_mem[order_id] = {
                "order_number": order_number,
                "state": state,
                "ms": {"dry": True},
            }
            continue

        log(logger, logging.INFO, "ms.create_customerorder", op="ms.create", order_id=order_id, order_number=order_number)
        co = ms.create_customerorder(body)
        created += 1

        supplies_mem[order_id] = {
            "order_number": order_number,
            "state": state,
            "ms": {
                "id": co.get("id"),
                "href": (co.get("meta") or {}).get("href"),
            },
        }

    return created, skipped
