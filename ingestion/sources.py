"""Request boundaries and validation for the initial raw collection."""
import calendar
import json
import re
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from urllib.parse import quote, unquote, urlencode

START = date(2021, 1, 1)
END = date(2026, 11, 23)
CODE_OPS = ("wholesaleMarkets", "units", "sizes", "corps", "placeOrigins", "packagings", "grades", "goods")
FUEL_CODES = frozenset({"B027", "B034", "D047", "C004", "K015"})
SETTLEMENT_OP = "Grid_20240625000000000653_1"
OPS = {"DS-001": SETTLEMENT_OP, "DS-002": "getWthrDataList",
       "DS-003": "getItemtradeList", "DS-004": "dateAvgRecentPrice.do"}
URLS = {"DS-002": "https://apis.data.go.kr/1360000/AsosDalyInfoService/",
        "DS-003": "https://apis.data.go.kr/1220000/Itemtrade/",
        "DS-004": "https://www.opinet.co.kr/api/",
        "DS-005": "https://apis.data.go.kr/B552845/katCode/"}
PAGE_SIZE = {"DS-001": 1000, "DS-002": 366, "DS-005": 1000}


class CollectionError(Exception):
    pass


def require(condition, message):
    if not condition:
        raise CollectionError(message)


def month_end(day):
    return day.replace(day=calendar.monthrange(day.year, day.month)[1])


def jobs(source, snapshot_date, markets=(), stations=(), hs_codes=(), *, hs_chapters=None):
    if source == "DS-005":
        for op in CODE_OPS:
            yield op, snapshot_date, ""
        return
    day = END
    while day >= START:
        if source == "DS-001":
            for market in markets:
                yield OPS[source], day.isoformat(), market
            day -= timedelta(days=1)
        elif source == "DS-004":
            yield OPS[source], day.isoformat(), "ALL"
            day -= timedelta(days=7)
        else:
            if source == "DS-002" or day < END:
                period = day.strftime("%Y-%m")
                codes = stations if source == "DS-002" else hs_codes
                if source == "DS-003" and hs_chapters is not None:
                    codes = ["ALL", *hs_chapters(period)]
                for code in codes:
                    yield OPS[source], period, code
            day = day.replace(day=1) - timedelta(days=1)


def settlement_range(page):
    size = PAGE_SIZE["DS-001"]
    return (page - 1) * size + 1, page * size


def request(source, op, period, entity, page, env, *, page_size=None):
    if source == "DS-001":
        first, last = settlement_range(page)
        filters = {"SALEDATE": period.replace("-", ""), "WHSALCD": entity}
        key = env.get("MAFRA_API_KEY")
        require(key and key.strip().lower() != "sample", "MAFRA_API_KEY required")
        url = f"http://211.237.50.150:7080/openapi/{quote(unquote(key), safe='')}/json/{op}/{first}/{last}"
        return url + "?" + urlencode(filters), filters | {"START_INDEX": first, "END_INDEX": last}
    params = {}
    if source in PAGE_SIZE:
        params.update(pageNo=page, numOfRows=page_size or PAGE_SIZE[source])
    if source == "DS-005":
        params["returnType"] = "json"
    if source == "DS-002":
        first = date.fromisoformat(period + "-01")
        params.update(dataCd="ASOS", dateCd="DAY", dataType="JSON", stnIds=entity,
                      startDt=first.strftime("%Y%m%d"), endDt=min(END, month_end(first)).strftime("%Y%m%d"))
    elif source == "DS-003":
        params.update(strtYymm=period.replace("-", ""), endYymm=period.replace("-", ""))
        if entity != "ALL":
            params["hsSgn"] = entity
    elif source == "DS-004":
        params.update(out="json", date=period.replace("-", ""))
        if entity != "ALL":
            require(entity in FUEL_CODES, "unknown Opinet product")
            params["prodcd"] = entity
    credential = ({"code": env["OPINET_API_KEY"]} if source == "DS-004"
                  else {"serviceKey": unquote(env["DATA_GO_KR_SERVICE_KEY"])})
    return URLS[source] + op + "?" + urlencode(params | credential), params


def api_error(code):
    # Provider messages can echo request URLs. Store only a short alphanumeric code.
    safe = str(code).replace("_", "").replace("-", "")
    raise CollectionError("API " + (str(code) if safe.isalnum() and len(safe) <= 32 else "error"))


TRADE_MEASURES = ("impWgt", "expWgt", "impDlr", "expDlr", "balPayments")


def trade_rows(raw, period, entity):
    root = ET.fromstring(raw)
    code = root.findtext("header/resultCode")
    if code != "00":
        api_error(code or root.findtext(".//returnReasonCode") or "missing-header")
    items = root.find("body/items")
    require(items is not None, "missing customs items")
    rows, totals = {}, []
    for item in items.findall("item"):
        r = {field.tag: field.text for field in item}
        if r.get("year") == "총계":
            totals.append(r)
            continue
        hs = r["hsCode"]
        # The nationwide control for 2022-01 includes a nine-digit source value.
        # Keep it verbatim for reconciliation. Agricultural detail must be ten digits.
        pattern = r"[0-9]{9,10}" if entity == "ALL" else r"[0-9]{10}"
        require(re.fullmatch(pattern, hs) and (entity == "ALL" or hs.startswith(entity))
                and r["year"] == period.replace("-", ".") and hs not in rows,
                "unexpected or duplicate customs month/HS code")
        rows[hs] = r
    require(len(totals) == 1 if rows else len(totals) <= 1, "missing or duplicate customs total")
    if totals:
        for field in TRADE_MEASURES:
            require(sum(int(r[field]) for r in rows.values()) == int(totals[0][field]),
                    f"customs total mismatch for {field}")
    return rows


def parse(source, period, entity, page, raw, *, page_size=None):
    if source == "DS-003":
        rows = trade_rows(raw, period, entity)
        return len(rows), len(rows)
    if raw.lstrip().startswith(b"<"):
        root = ET.fromstring(raw)
        api_error(root.findtext(".//returnReasonCode") or root.findtext(".//resultCode")
                  or root.findtext(".//code") or "unexpected-xml")
    data = json.loads(raw)
    if source == "DS-001":
        body = data.get(SETTLEMENT_OP, data)
        code = body.get("result", {}).get("code")
        if code != "INFO-000":
            api_error(code or "missing-settlement-result")
        first, last = settlement_range(page)
        total = int(body["totalCnt"])
        rows = body.get("row", [])
        expected = max(0, min(last, total) - first + 1)
        require(total >= 0 and int(body["startRow"]) == first and int(body["endRow"]) in (last, min(total, last))
                and len(rows) == expected, "settlement page count mismatch")
        require(all(int(r["ROW_NUM"]) == i and r["SALEDATE"] == period.replace("-", "")
                    and str(r["WHSALCD"]) == entity for i, r in enumerate(rows, first)),
                "unexpected settlement row number, market or day")
        return len(rows), total
    if source == "DS-004":
        rows = data["RESULT"]["OIL"]
        end = date.fromisoformat(period)
        products = FUEL_CODES if entity == "ALL" else {entity}
        expected = {((end - timedelta(days=n)).strftime("%Y%m%d"), product)
                    for n in range(7) for product in products}
        require(rows, "empty Opinet response")
        require(len(rows) == len(expected) and {(r["DATE"], r["PRODCD"]) for r in rows} == expected,
                "incomplete Opinet dates or products")
        return len(rows), len(rows)
    response = data["response"]
    code = str(response["header"]["resultCode"])
    if source == "DS-002" and code == "03":
        return 0, 0
    if code not in ("0", "00"):
        api_error(code)
    body = response["body"]
    total = int(body["totalCount"])
    rows = (body.get("items") or {}).get("item", [])
    response_size = int(body["numOfRows"])
    size = page_size or PAGE_SIZE[source]
    require(total >= 0 and size > 0 and response_size == size and int(body["pageNo"]) == page and
            len(rows) == min(size, max(0, total - (page - 1) * size)), "page count mismatch")
    if source == "DS-002":
        dates = {r["tm"] for r in rows}
        require(len(dates) == len(rows) and all(d.startswith(period + "-") and d <= END.isoformat() for d in dates)
                and all(str(r["stnId"]) == entity for r in rows), "unexpected observation station or date")
    return len(rows), total
