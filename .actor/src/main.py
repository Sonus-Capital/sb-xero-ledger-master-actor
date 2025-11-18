import csv
import io
import json
import urllib.request
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Set, Tuple

from apify import Actor


# ------------- helpers -------------

def norm(value: Any) -> str:
    """Normalise a value to a trimmed string (never None)."""
    if value is None:
        return ""
    return str(value).strip()


def safe_decimal(value: Any) -> Decimal:
    """Parse numeric strings safely into Decimal; treat blanks as 0."""
    s = norm(value)
    if not s:
        return Decimal("0")
    # Strip thousands separators
    s = s.replace(",", "")
    try:
        return Decimal(s)
    except InvalidOperation:
        return Decimal("0")


def download_csv(url: str, label: str) -> List[Dict[str, Any]]:
    """
    Download a CSV from Dropbox and parse it into a list of dicts.

    First try csv.DictReader; if it fails due to newline/quoting issues,
    fall back to a naive split-based parser.
    """
    if not url:
        return []

    Actor.log.info(f"Downloading {label} CSV from {url}")

    with urllib.request.urlopen(url) as resp:
        csv_bytes = resp.read()

    text = csv_bytes.decode("utf-8", errors="replace")

    rows: List[Dict[str, Any]] = []

    # Primary attempt: DictReader
    try:
        reader = csv.DictReader(io.StringIO(text))
        rows = [dict(r) for r in reader]
        Actor.log.info(f"{label} rows (DictReader): {len(rows)}")
        return rows
    except csv.Error as e:
        Actor.log.warning(
            f"{label} CSV parse via DictReader failed: {e!r}. "
            f"Falling back to simple split parser; some rows may be skipped."
        )

    # Fallback: naive split parser
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        Actor.log.warning(f"{label} CSV has no non-empty lines after fallback parsing.")
        return []

    header = [h.strip() for h in lines[0].split(",")]
    for ln in lines[1:]:
        parts = [p.strip() for p in ln.split(",")]
        if len(parts) != len(header):
            # Skip malformed lines
            continue
        row = dict(zip(header, parts))
        rows.append(row)

    Actor.log.info(f"{label} rows (fallback): {len(rows)}")
    return rows


# ---- key extractors ----

def ledger_invoice_key(row: Dict[str, Any]) -> str:
    """
    Extract an invoice reference from the ledger / master financials.

    Typical Master_Financials headers (from your 2016 example):
    - Invoice Number
    - Invoice number
    - Invoice
    """
    return norm(
        row.get("Invoice Number")
        or row.get("Invoice number")
        or row.get("Invoice")
    ).upper()


def invoice_guid_key(row: Dict[str, Any]) -> str:
    """
    Extract the Xero invoice GUID from the Invoice Master / Enriched files.

    Common header names (from your invoice/enriched masters):
    - Invoice ID
    - InvoiceId
    - Invoice_GUID
    """
    return norm(
        row.get("Invoice ID")
        or row.get("InvoiceId")
        or row.get("Invoice_GUID")
    ).lower()


def invoice_number_key(row: Dict[str, Any]) -> str:
    """
    Extract human invoice number from Invoice Master / Enriched.

    Headers:
    - Xero number
    - Invoice Number
    - Invoice number
    - Invoice
    """
    return norm(
        row.get("Xero number")
        or row.get("Invoice Number")
        or row.get("Invoice number")
        or row.get("Invoice")
    ).upper()


def manifest_guid_key(row: Dict[str, Any]) -> str:
    """
    Extract invoice GUID from the Invoice Manifest.

    Expected headers from manifest:
    - Invoice ID
    - InvoiceId
    - Invoice_GUID
    """
    return norm(
        row.get("Invoice ID")
        or row.get("InvoiceId")
        or row.get("Invoice_GUID")
    ).lower()


def manifest_pdf_path(row: Dict[str, Any]) -> str:
    """
    Extract a PDF path / filename from manifest rows.

    Expected possible headers:
    - PDF_Path
    - PdfPath
    - FilePath
    - Path
    """
    return norm(
        row.get("PDF_Path")
        or row.get("PdfPath")
        or row.get("FilePath")
        or row.get("Path")
    )


def sum_field(
    rows: List[Dict[str, Any]],
    candidates: List[str],
) -> Decimal:
    """
    Sum a numeric field across rows, trying multiple header names in order.

    e.g. candidates = ["Gross (AUD)", "Gross", "Gross (Source)"].
    """
    total = Decimal("0")
    for row in rows:
        for col in candidates:
            if col in row and norm(row[col]) != "":
                total += safe_decimal(row[col])
                break
    return total


# ------------- main logic -------------


async def main() -> None:
    async with Actor:
        actor_input = await Actor.get_input() or {}
        Actor.log.info(f"Actor input keys: {list(actor_input.keys())}")

        year = norm(actor_input.get("Year"))
        ledger_url = norm(actor_input.get("LedgerUrl"))
        invoice_url = norm(actor_input.get("InvoiceUrl"))
        manifest_url = norm(actor_input.get("ManifestUrl"))

        if not ledger_url or not invoice_url or not manifest_url:
            Actor.log.error(
                "Missing one or more required URLs. "
                "Expected LedgerUrl, InvoiceUrl, ManifestUrl."
            )
            return

        # 1) Download all three sources
        ledger_rows = download_csv(ledger_url, "ledger")
        invoice_rows = download_csv(invoice_url, "invoice")
        manifest_rows = download_csv(manifest_url, "manifest")

        Actor.log.info(
            f"Row counts: ledger={len(ledger_rows)}, "
            f"invoices={len(invoice_rows)}, manifest={len(manifest_rows)}"
        )

        if not (ledger_rows or invoice_rows or manifest_rows):
            Actor.log.error("All inputs are empty; nothing to do.")
            return

        # 2) Build indices

        # Ledger indexed by invoice number
        ledger_by_invnum: Dict[str, List[Dict[str, Any]]] = {}
        for r in ledger_rows:
            k = ledger_invoice_key(r)
            if not k:
                continue
            ledger_by_invnum.setdefault(k, []).append(r)

        # Invoices indexed by GUID and by invoice number
        invoice_by_guid: Dict[str, List[Dict[str, Any]]] = {}
        invoice_by_invnum: Dict[str, List[Dict[str, Any]]] = {}
        for r in invoice_rows:
            g = invoice_guid_key(r)
            n = invoice_number_key(r)
            if g:
                invoice_by_guid.setdefault(g, []).append(r)
            if n:
                invoice_by_invnum.setdefault(n, []).append(r)

        # Manifest indexed by GUID
        manifest_by_guid: Dict[str, List[Dict[str, Any]]] = {}
        for r in manifest_rows:
            g = manifest_guid_key(r)
            if not g:
                continue
            manifest_by_guid.setdefault(g, []).append(r)

        # 3) Build unified key space
        # Use invoice GUID as the primary key where possible
        guid_keys: Set[str] = set(invoice_by_guid.keys()) | set(manifest_by_guid.keys())

        # Per-invoice ledger master rows
        master_rows: List[Dict[str, Any]] = []

        for guid in sorted(guid_keys):
            inv_rows = invoice_by_guid.get(guid, [])
            man_rows = manifest_by_guid.get(guid, [])

            # We may have multiple invoice rows per GUID (line items)
            first_inv = inv_rows[0] if inv_rows else {}

            inv_num = invoice_number_key(first_inv)
            inv_contact = norm(first_inv.get("Contact"))
            inv_desc = norm(first_inv.get("Description"))
            inv_date = norm(first_inv.get("Date"))
            inv_type = norm(first_inv.get("Type"))
            inv_currency = norm(first_inv.get("Currency"))
            inv_line_total = sum_field(inv_rows, ["Line amount", "Line Amount", "Line total", "Line Total"])
            inv_tax_total = sum_field(inv_rows, ["Tax amount", "Tax Amount", "GST", "GST (AUD)"])

            # Ledger rows for this invoice (join via invoice number)
            ledger_rows_for_invoice = ledger_by_invnum.get(inv_num, []) if inv_num else []

            ledger_gross = sum_field(
                ledger_rows_for_invoice,
                ["Gross (AUD)", "Gross", "Gross (Source)"],
            )
            ledger_net = sum_field(
                ledger_rows_for_invoice,
                ["Net (AUD)", "Net", "Net (Source)"],
            )
            ledger_gst = sum_field(
                ledger_rows_for_invoice,
                ["GST (AUD)", "GST", "GST (Source)"],
            )

            # Manifest / PDF info
            pdf_paths = sorted(
                {
                    manifest_pdf_path(r)
                    for r in man_rows
                    if manifest_pdf_path(r)
                }
            )
            pdf_present = bool(pdf_paths)

            master_rows.append(
                {
                    "Year": year,
                    "Invoice_GUID": guid,
                    "Invoice_Number": inv_num,
                    "Invoice_Type": inv_type,
                    "Invoice_Date": inv_date,
                    "Invoice_Contact": inv_contact,
                    "Invoice_Description": inv_desc,
                    "Invoice_Currency": inv_currency,
                    "Invoice_Line_Count": len(inv_rows),
                    "Invoice_Line_Total": f"{inv_line_total:.2f}",
                    "Invoice_Tax_Total": f"{inv_tax_total:.2f}",

                    "In_Ledger": "Y" if ledger_rows_for_invoice else "N",
                    "Ledger_Row_Count": len(ledger_rows_for_invoice),
                    "Ledger_Gross_AUD": f"{ledger_gross:.2f}",
                    "Ledger_Net_AUD": f"{ledger_net:.2f}",
                    "Ledger_GST_AUD": f"{ledger_gst:.2f}",

                    "In_Manifest": "Y" if man_rows else "N",
                    "Manifest_Row_Count": len(man_rows),
                    "PDF_Present": "Y" if pdf_present else "N",
                    "PDF_Count": len(pdf_paths),
                    "PDF_Paths": "; ".join(pdf_paths),
                }
            )

        # 4) Ledger-only invoice numbers (no invoice master / manifest)
        invnums_from_guid = {
            invoice_number_key(rows[0])
            for rows in invoice_by_guid.values()
            if rows
        }
        invnums_from_ledger = set(ledger_by_invnum.keys())

        ledger_only_invnums = invnums_from_ledger - invnums_from_guid

        for inv_num in sorted(ledger_only_invnums):
            l_rows = ledger_by_invnum.get(inv_num, [])

            ledger_gross = sum_field(
                l_rows,
                ["Gross (AUD)", "Gross", "Gross (Source)"],
            )
            ledger_net = sum_field(
                l_rows,
                ["Net (AUD)", "Net", "Net (Source)"],
            )
            ledger_gst = sum_field(
                l_rows,
                ["GST (AUD)", "GST", "GST (Source)"],
            )

            # Sample description/contact from ledger
            first_l = l_rows[0] if l_rows else {}
            ledger_contact = norm(first_l.get("Contact"))
            ledger_desc = norm(first_l.get("Description"))

            master_rows.append(
                {
                    "Year": year,
                    "Invoice_GUID": "",
                    "Invoice_Number": inv_num,
                    "Invoice_Type": "",
                    "Invoice_Date": "",
                    "Invoice_Contact": ledger_contact,
                    "Invoice_Description": ledger_desc,
                    "Invoice_Currency": norm(first_l.get("Currency")),
                    "Invoice_Line_Count": 0,
                    "Invoice_Line_Total": "0.00",
                    "Invoice_Tax_Total": "0.00",

                    "In_Ledger": "Y",
                    "Ledger_Row_Count": len(l_rows),
                    "Ledger_Gross_AUD": f"{ledger_gross:.2f}",
                    "Ledger_Net_AUD": f"{ledger_net:.2f}",
                    "Ledger_GST_AUD": f"{ledger_gst:.2f}",

                    "In_Manifest": "N",
                    "Manifest_Row_Count": 0,
                    "PDF_Present": "N",
                    "PDF_Count": 0,
                    "PDF_Paths": "",
                }
            )

        # 5) Write CSV to KV store
        filename_year = year if year else "unknown"
        kv_filename = f"ledger_master_{filename_year}.csv"

        fieldnames = [
            "Year",
            "Invoice_GUID",
            "Invoice_Number",
            "Invoice_Type",
            "Invoice_Date",
            "Invoice_Contact",
            "Invoice_Description",
            "Invoice_Currency",
            "Invoice_Line_Count",
            "Invoice_Line_Total",
            "Invoice_Tax_Total",
            "In_Ledger",
            "Ledger_Row_Count",
            "Ledger_Gross_AUD",
            "Ledger_Net_AUD",
            "Ledger_GST_AUD",
            "In_Manifest",
            "Manifest_Row_Count",
            "PDF_Present",
            "PDF_Count",
            "PDF_Paths",
        ]

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in master_rows:
            writer.writerow(row)

        csv_data = buf.getvalue()

        await Actor.set_value(
            kv_filename,
            csv_data,
            content_type="text/csv; charset=utf-8",
        )

        summary = {
            "year": year,
            "ledger_rows": len(ledger_rows),
            "invoice_rows": len(invoice_rows),
            "manifest_rows": len(manifest_rows),
            "guid_keys": len(guid_keys),
            "ledger_only_invoice_numbers": len(ledger_only_invnums),
            "master_rows": len(master_rows),
            "kv_filename": kv_filename,
        }
        await Actor.push_data(summary)

        Actor.log.info(
            f"Done. year={year}, master_rows={len(master_rows)}, "
            f"ledger_only_invnums={len(ledger_only_invnums)}, "
            f"kv_file={kv_filename}"
        )
