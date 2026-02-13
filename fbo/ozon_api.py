from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .http_client import JsonHttpClient


@dataclass(frozen=True)
class SupplyListItem:
    order_id: int
    order_number: str
    state: str
    created_date: Optional[str] = None
    state_updated_date: Optional[str] = None


class OzonApi:
    def __init__(self, client: JsonHttpClient):
        self.c = client

    def list_supply_orders(
        self,
        states: List[str],
        limit: int = 100,
        last_id: Optional[str] = None,
        sort_by: str = "TIMESLOT_FROM_UTC",
        sort_dir: str = "DESC",
    ) -> Dict[str, Any]:
        """Raw /v3/supply-order/list.

        Ozon API shape may evolve; we keep it flexible and log errors upstream.
        """
        body: Dict[str, Any] = {
            "filter": {
                "states": states,
            },
            "limit": limit,
            "sort_by": sort_by,
            "sort_dir": sort_dir,
        }
        if last_id:
            body["last_id"] = last_id
        return self.c.request("POST", "/v3/supply-order/list", json_body=body)

    def details(self, order_id: int) -> Dict[str, Any]:
        return self.c.request("POST", "/v1/supply-order/details", json_body={"order_id": order_id})

    def bundle_items(self, bundle_id: str, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        # bundle_ids is required array; limit is required 1..100
        body = {"bundle_ids": [bundle_id], "limit": limit, "offset": offset}
        return self.c.request("POST", "/v1/supply-order/bundle", json_body=body)

    def bundle_items_all(self, bundle_id: str) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        offset = 0
        while True:
            resp = self.bundle_items(bundle_id=bundle_id, limit=100, offset=offset)
            chunk = resp.get("items") or []
            items.extend(chunk)
            has_next = bool(resp.get("has_next"))
            if not has_next:
                break
            # last_id is sku usually; but we paginate by offset for simplicity
            offset += len(chunk)
            if len(chunk) == 0:
                break
        return items
