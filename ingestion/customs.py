"""KATI agricultural HS codes and the Customs monthly item-trade response."""
import csv
import hashlib
import json
import re
from urllib.parse import urlencode
from urllib.request import Request

from ingestion.sources import OPS, START, TRADE_MEASURES, jobs, require, trade_rows


KATI_PAGE = "https://www.kati.net/statistics/monthlyPerformanceByProduct.do"
KATI_ITEMS = "https://www.kati.net/stat/search/item/search.do"
KATI_PARAMS = {"ssi_digits": "9", "ssi_see_together": "Y"}


def agriculture_catalog(directory, fetch):
    """Pin the original reference response per run, including retired codes."""
    directory.mkdir(parents=True, exist_ok=True)
    snapshots = list(directory.glob("kati-items-*.json"))
    if snapshots:
        require(len(snapshots) == 1, "ambiguous KATI catalog snapshot")
        raw = snapshots[0].read_bytes()
        require(snapshots[0].stem == "kati-items-" + hashlib.sha256(raw).hexdigest(),
                "KATI catalog checksum mismatch")
    else:
        req = Request(KATI_ITEMS, data=urlencode(KATI_PARAMS).encode(),
                      headers={"Referer": KATI_PAGE, "X-Requested-With": "XMLHttpRequest"})
        raw = fetch(req)
    rows = json.loads(raw)["list"]
    require({r["grp_cd"] for r in rows} >= {"1", "2", "3", "4"}, "incomplete KATI groups")
    by_hs = {}
    for row in rows:
        if row["grp_cd"] == "1":
            hs, end = row["hscd"], row["first_regdtime"]  # KATI labels this field 종료일.
            require(re.fullmatch(r"[0-9]{10}", hs) and re.fullmatch(r"[0-9]{4}-(0[1-9]|1[0-2])", end),
                    "unexpected KATI HS code or end month")
            by_hs.setdefault(hs, []).append(row)
    require(by_hs, "empty KATI agriculture group")
    if not snapshots:
        target = directory / ("kati-items-" + hashlib.sha256(raw).hexdigest() + ".json")
        tmp = target.with_suffix(".part")
        tmp.write_bytes(raw)
        tmp.replace(target)
    return by_hs


def chapters(catalog, month):
    return sorted({hs[:2] for hs, entries in catalog.items()
                   if any(month <= r["first_regdtime"] for r in entries)})


def export_trade(db, run, catalog, directory, read_page):
    """Check monthly chapter responses against the nationwide response, then export once per HS."""
    months = sorted(period for _, period, _ in jobs("DS-003", "", hs_codes=("ALL",)))
    pages = {(r["period"], r["entity"]): r for r in db.execute("""
        SELECT u.period,u.entity,p.* FROM units u JOIN pages p ON p.unit=u.id AND p.revision=u.revision
        WHERE u.run=? AND u.source='DS-003' AND u.op=? AND u.status IN ('complete','empty')
        AND p.page=1 AND (u.entity='ALL' OR length(u.entity)=2)""", (run, OPS["DS-003"]))}
    expected = {(m, h) for m in months for h in ["ALL", *chapters(catalog, m)]}
    require(expected <= pages.keys(), f"missing {len(expected - pages.keys())} monthly customs responses")
    directory.mkdir(parents=True, exist_ok=True)
    monthly_path = directory / "agricultural-trade-monthly.csv"
    codes_path = directory / "agricultural-hs-coverage.csv"
    monthly_tmp = monthly_path.with_suffix(".part")
    codes_tmp = codes_path.with_suffix(".part")
    observed = {}
    with monthly_tmp.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["month", "hsCode", "agCode", "itemName", "statKor", *TRADE_MEASURES])
        for month in months:
            nationwide = trade_rows(read_page(pages[month, "ALL"]), month, "ALL")
            require(nationwide, f"empty nationwide customs response: {month}")
            for chapter in chapters(catalog, month):
                detail = trade_rows(read_page(pages[month, chapter]), month, chapter)
                require(detail == {hs: r for hs, r in nationwide.items() if hs.startswith(chapter)},
                        f"nationwide/chapter mismatch: {month} HS {chapter}")
                for hs, row in sorted(detail.items()):
                    matches = [r for r in catalog.get(hs, ()) if month <= r["first_regdtime"]]
                    if not matches:
                        continue
                    require(len(matches) == 1, f"ambiguous KATI category: {month} {hs}")
                    ref = matches[0]
                    writer.writerow([month, hs, ref["agcd"], ref["name"], row["statKor"],
                                     *(row[key] for key in TRADE_MEASURES)])
                    observed.setdefault(hs, []).append(month)
    with codes_tmp.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["hsCode", "agCode", "itemName", "katiEndMonth", "observedMonths",
                         "firstObservedMonth", "lastObservedMonth"])
        for hs, entries in sorted(catalog.items()):
            for r in entries:
                if r["first_regdtime"] >= START.strftime("%Y-%m"):
                    actual = observed.get(hs, [])
                    writer.writerow([hs, r["agcd"], r["name"], r["first_regdtime"], len(actual),
                                     actual[0] if actual else "", actual[-1] if actual else ""])
    monthly_tmp.replace(monthly_path)
    codes_tmp.replace(codes_path)
    print(f"DS-003 verified {len(expected)} source responses, {len(months)} months, "
          f"{len(observed)} agricultural HS codes, {sum(map(len, observed.values()))} month/HS rows",
          flush=True)


