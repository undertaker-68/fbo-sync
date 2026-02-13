from __future__ import annotations

from typing import Any, Dict, Optional

from .http_client import JsonHttpClient


def meta(href: str, type_: str) -> Dict[str, Any]:
    return {"meta": {"href": href, "type": type_, "mediaType": "application/json"}}


class MsApi:
    def __init__(self, client: JsonHttpClient):
        self.c = client

    def find_customerorder_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        # name is unique for our sync; if exists we skip&forget
        filt = f"name={name}"
        resp = self.c.request("GET", "/entity/customerorder", params={"filter": filt, "limit": 1, "offset": 0})
        rows = resp.get("rows") or []
        return rows[0] if rows else None

    def create_customerorder(self, body: Dict[str, Any]) -> Dict[str, Any]:
        return self.c.request("POST", "/entity/customerorder", json_body=body)

    def search_product_by_article(self, article: str) -> Optional[Dict[str, Any]]:
        resp = self.c.request("GET", "/entity/product", params={"filter": f"article={article}", "limit": 1, "offset": 0})
        rows = resp.get("rows") or []
        return rows[0] if rows else None

    def search_bundle_by_article(self, article: str) -> Optional[Dict[str, Any]]:
        resp = self.c.request("GET", "/entity/bundle", params={"filter": f"article={article}", "limit": 1, "offset": 0})
        rows = resp.get("rows") or []
        return rows[0] if rows else None

    def get_product(self, product_id: str) -> Dict[str, Any]:
        return self.c.request("GET", f"/entity/product/{product_id}")

    def get_bundle(self, bundle_id: str) -> Dict[str, Any]:
        return self.c.request("GET", f"/entity/bundle/{bundle_id}")

    def get_bundle_components(self, bundle_id: str) -> Dict[str, Any]:
        return self.c.request("GET", f"/entity/bundle/{bundle_id}/components", params={"limit": 1000, "offset": 0})
