"""Full data processing pipeline for commodity market intelligence.

Stages: Validation → Deduplication → Cleaning → Normalization →
        Translation → Entity Recognition → Commodity Classification → Sector Classification
"""
import asyncio
import hashlib
import json
import logging
import re
import time
import unicodedata
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Any, Callable, Dict, List, Optional, Tuple

from commodity_os.core.events import Event, EventType, event_bus

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REQUIRED_FIELDS = [
    "company_name",
    "product",
    "entity_type",
    "contact_phone",
    "address",
    "district",
    "state",
]

VALID_ENTITY_TYPES = {"manufacturer", "wholesaler", "exporter"}
VALID_PRODUCTS = {
    "sugar", "rice", "grains", "pulses", "wheat", "dals", "basmathi rice",
    "basmati rice", "basmathi", "basmati",
}

UNIT_CONVERSIONS: Dict[str, float] = {
    "kg": 1.0,
    "kgs": 1.0,
    "kilogram": 1.0,
    "kilograms": 1.0,
    "quintal": 100.0,
    "quintals": 100.0,
    "qtl": 100.0,
    "mt": 1000.0,
    "metric ton": 1000.0,
    "metric tons": 1000.0,
    "tonne": 1000.0,
    "tonnes": 1000.0,
    "ton": 1000.0,
}

PHONE_CLEANUP_RE = re.compile(r"[^\d+]")
GST_PATTERN = re.compile(
    r"\b\d{2}[A-Z]{5}\d{4}[A-Z]{1}[A-Z\d]{1}Z[A-Z\d]{1}\b"
)
PHONE_PATTERN = re.compile(r"(?:\+?91[\s-]?)?[6-9]\d{9}")
DATE_PATTERNS = [
    (re.compile(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{4})\b"), "%d/%m/%Y"),
    (re.compile(r"\b(\d{4})[/-](\d{1,2})[/-](\d{1,2})\b"), "%Y/%m/%d"),
    (re.compile(r"\b(\w+)\s+(\d{4})\b"), "%B %Y"),
]

COMMODITY_KEYWORDS: Dict[str, List[str]] = {
    "sugar": ["sugar", "gur", "jaggery", "cane sugar", "raw sugar", "refined sugar"],
    "rice": ["rice", "paddy", "non-basmati", "parboiled rice", "raw rice"],
    "grains": ["grain", "grains", "cereal", "cereals", "maize", "corn", "millet", "bajra", "jowar", "sorghum"],
    "pulses": ["pulse", "pulses", "legume", "legumes"],
    "wheat": ["wheat", "atta", "flour", "godambu"],
    "dals": ["dal", "dals", "lentil", "lentils", "toor", "moong", "chana", "urad", "masoor"],
    "basmathi rice": ["basmati", "basmathi", "basumati", "1121", "pusa basmati", "traditional basmati"],
}

SECTOR_KEYWORDS: Dict[str, List[str]] = {
    "manufacturer": ["manufacturer", "manufacturing", "producer", "production", "factory", "mill", "refinery", "processor", "processor"],
    "wholesaler": ["wholesaler", "wholesale", "dealer", "distributor", "trader", "merchant", "stockist", "supplier"],
    "exporter": ["export", "exporter", "exporting", "overseas", "international trade", "foreign trade"],
}

SIMILARITY_THRESHOLD = 0.85


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class StageStats:
    """Metrics for a single pipeline stage."""
    stage_name: str
    input_count: int = 0
    output_count: int = 0
    errors: int = 0
    skipped: int = 0
    processing_time_ms: float = 0.0
    error_details: List[str] = field(default_factory=list)

    @property
    def drop_rate(self) -> float:
        if self.input_count == 0:
            return 0.0
        return (self.input_count - self.output_count) / self.input_count * 100


@dataclass
class PipelineResult:
    """Aggregate result returned after running the full pipeline."""
    records_in: int = 0
    records_out: int = 0
    total_processing_time_ms: float = 0.0
    stages: List[StageStats] = field(default_factory=list)
    succeeded: bool = True
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "records_in": self.records_in,
            "records_out": self.records_out,
            "total_processing_time_ms": self.total_processing_time_ms,
            "succeeded": self.succeeded,
            "errors": self.errors,
            "stages": [
                {
                    "stage": s.stage_name,
                    "input": s.input_count,
                    "output": s.output_count,
                    "errors": s.errors,
                    "drop_rate_pct": round(s.drop_rate, 2),
                    "time_ms": round(s.processing_time_ms, 2),
                }
                for s in self.stages
            ],
        }


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _content_hash(record: Dict[str, Any]) -> str:
    """Deterministic hash of the record content for dedup."""
    canonical = json.dumps(
        {k: record.get(k) for k in sorted(record.keys())},
        sort_keys=True,
        default=str,
        ensure_ascii=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _fuzzy_ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def _normalize_text(text: Optional[str]) -> str:
    if text is None:
        return ""
    text = str(text).strip()
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"\s+", " ", text)
    return text


def _normalize_phone(phone: Optional[str]) -> str:
    if phone is None:
        return ""
    cleaned = PHONE_CLEANUP_RE.sub("", str(phone))
    cleaned = cleaned.lstrip("0")
    if len(cleaned) == 10 and cleaned[0] in "6789":
        return "+91" + cleaned
    if cleaned.startswith("91") and len(cleaned) == 12:
        return "+" + cleaned
    return "+" + cleaned if cleaned else ""


def _standardize_unit(record: Dict[str, Any]) -> Dict[str, Any]:
    """Convert quantity to base unit (KG)."""
    unit_raw = (record.get("quantity_unit") or record.get("unit") or "kg").lower().strip()
    qty = record.get("quantity") or record.get("qty") or 0
    try:
        qty = float(qty)
    except (TypeError, ValueError):
        qty = 0.0

    factor = UNIT_CONVERSIONS.get(unit_raw, 1.0)
    record["quantity_kg"] = qty * factor
    record["quantity_unit_original"] = unit_raw
    return record


def _transliterate_local(text: str) -> str:
    """Strip non-ASCII characters that represent local-language glyphs."""
    if not text:
        return text
    normalized = unicodedata.normalize("NFKD", text)
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^A-Za-z0-9\s,.\-/&'()]", "", ascii_only)
    return re.sub(r"\s+", " ", cleaned).strip()


# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------

class _StageBase:
    """Mixin that records timing and emits an event."""

    event_type: EventType = EventType.DATA_VALIDATED
    stage_name: str = "base"

    def _emit_event(self, payload: Dict[str, Any]):
        asyncio.get_event_loop().create_task(
            event_bus.emit(self.event_type, payload, source=f"pipeline.{self.stage_name}")
        )

    def _make_stats(self) -> StageStats:
        return StageStats(stage_name=self.stage_name)


class ValidationStage(_StageBase):
    event_type = EventType.DATA_VALIDATED
    stage_name = "validation"

    def run(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        stats = self._make_stats()
        stats.input_count = len(records)
        valid: List[Dict[str, Any]] = []
        t0 = time.perf_counter()

        for rec in records:
            errors: List[str] = []
            for f in REQUIRED_FIELDS:
                val = rec.get(f)
                if val is None or (isinstance(val, str) and not val.strip()):
                    errors.append(f"missing:{f}")

            phone = rec.get("contact_phone") or rec.get("phone") or ""
            if phone and not PHONE_PATTERN.search(str(phone)):
                errors.append("invalid_phone_format")

            if errors:
                stats.errors += 1
                stats.error_details.append(
                    f"{rec.get('company_name', '?')}: {'; '.join(errors)}"
                )
                rec["_validation_errors"] = errors
            else:
                rec.pop("_validation_errors", None)
                valid.append(rec)

        stats.output_count = len(valid)
        stats.processing_time_ms = (time.perf_counter() - t0) * 1000
        self._emit_event({
            "total": stats.input_count,
            "valid": stats.output_count,
            "invalid": stats.errors,
            "time_ms": round(stats.processing_time_ms, 2),
        })
        return valid, stats


class DeduplicationStage(_StageBase):
    event_type = EventType.DATA_DEDUPED
    stage_name = "deduplication"

    def run(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        stats = self._make_stats()
        stats.input_count = len(records)
        t0 = time.perf_counter()

        seen_hashes: Dict[str, int] = {}
        unique: List[Dict[str, Any]] = []

        for rec in records:
            ch = _content_hash(rec)
            if ch in seen_hashes:
                seen_hashes[ch] += 1
                stats.skipped += 1
                continue

            dup_found = False
            for idx, existing in enumerate(unique):
                name_ratio = _fuzzy_ratio(
                    rec.get("company_name", ""), existing.get("company_name", "")
                )
                gst_match = (
                    rec.get("gst_number")
                    and rec.get("gst_number") == existing.get("gst_number")
                )
                addr_ratio = _fuzzy_ratio(
                    rec.get("address", ""), existing.get("address", "")
                )
                if gst_match or (name_ratio > SIMILARITY_THRESHOLD and addr_ratio > 0.6):
                    dup_found = True
                    stats.skipped += 1
                    break

            if not dup_found:
                seen_hashes[ch] = 1
                unique.append(rec)

        stats.output_count = len(unique)
        stats.processing_time_ms = (time.perf_counter() - t0) * 1000
        self._emit_event({
            "total": stats.input_count,
            "unique": stats.output_count,
            "duplicates_removed": stats.skipped,
            "time_ms": round(stats.processing_time_ms, 2),
        })
        return unique, stats


class CleaningStage(_StageBase):
    event_type = EventType.DATA_CLEANED
    stage_name = "cleaning"

    def run(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        stats = self._make_stats()
        stats.input_count = len(records)
        t0 = time.perf_counter()

        cleaned: List[Dict[str, Any]] = []
        for rec in records:
            try:
                for key in list(rec.keys()):
                    if isinstance(rec[key], str):
                        rec[key] = _normalize_text(rec[key])

                phone_key = "contact_phone" if "contact_phone" in rec else "phone"
                raw_phone = rec.get(phone_key, "")
                rec["phone_normalized"] = _normalize_phone(raw_phone)

                gst = rec.get("gst_number") or ""
                gst_match = GST_PATTERN.search(str(gst))
                rec["gst_valid"] = gst_match is not None
                if gst_match:
                    rec["gst_number"] = gst_match.group(0)

                if "notes" in rec and isinstance(rec["notes"], str):
                    rec["notes"] = rec["notes"].strip()[:2000]

                cleaned.append(rec)
            except Exception as exc:
                stats.errors += 1
                stats.error_details.append(str(exc))

        stats.output_count = len(cleaned)
        stats.processing_time_ms = (time.perf_counter() - t0) * 1000
        self._emit_event({
            "total": stats.input_count,
            "cleaned": stats.output_count,
            "errors": stats.errors,
            "time_ms": round(stats.processing_time_ms, 2),
        })
        return cleaned, stats


class NormalizationStage(_StageBase):
    event_type = EventType.DATA_NORMALIZED
    stage_name = "normalization"

    def run(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        stats = self._make_stats()
        stats.input_count = len(records)
        t0 = time.perf_counter()

        for rec in records:
            rec = _standardize_unit(rec)
            for price_key in ("market_price", "purchase_price", "selling_price", "price"):
                val = rec.get(price_key)
                if val is not None:
                    try:
                        rec[price_key] = round(float(val), 2)
                        rec[f"{price_key}_currency"] = "INR"
                    except (TypeError, ValueError):
                        pass

            for date_key in ("last_updated", "date", "established_year", "year_of_establishment"):
                val = rec.get(date_key)
                if val is not None:
                    rec[f"{date_key}_normalized"] = str(val)[:10]

            rec["entity_type"] = (rec.get("entity_type") or "").lower().strip()
            rec["product"] = (rec.get("product") or "").lower().strip()

        stats.output_count = len(records)
        stats.processing_time_ms = (time.perf_counter() - t0) * 1000
        self._emit_event({
            "total": stats.input_count,
            "normalized": stats.output_count,
            "time_ms": round(stats.processing_time_ms, 2),
        })
        return records, stats


class TranslationStage(_StageBase):
    event_type = EventType.DATA_TRANSLATED
    stage_name = "translation"

    def run(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        stats = self._make_stats()
        stats.input_count = len(records)
        t0 = time.perf_counter()

        for rec in records:
            for key in ("company_name", "product", "address", "district", "state", "contact_person"):
                val = rec.get(key)
                if val and isinstance(val, str) and any(ord(ch) > 127 for ch in val):
                    rec[f"{key}_original"] = val
                    rec[key] = _transliterate_local(val)

        stats.output_count = len(records)
        stats.processing_time_ms = (time.perf_counter() - t0) * 1000
        self._emit_event({
            "total": stats.input_count,
            "translated": stats.output_count,
            "time_ms": round(stats.processing_time_ms, 2),
        })
        return records, stats


class EntityRecognitionStage(_StageBase):
    event_type = EventType.ENTITY_RECOGNIZED
    stage_name = "entity_recognition"

    def run(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        stats = self._make_stats()
        stats.input_count = len(records)
        t0 = time.perf_counter()

        for rec in records:
            if not rec.get("gst_number"):
                all_text = " ".join(str(v) for v in rec.values() if isinstance(v, str))
                gst_match = GST_PATTERN.search(all_text)
                if gst_match:
                    rec["gst_number"] = gst_match.group(0)

            if not rec.get("contact_phone") and not rec.get("phone"):
                all_text = " ".join(str(v) for v in rec.values() if isinstance(v, str))
                phone_match = PHONE_PATTERN.search(all_text)
                if phone_match:
                    rec["contact_phone"] = phone_match.group(0)

        stats.output_count = len(records)
        stats.processing_time_ms = (time.perf_counter() - t0) * 1000
        self._emit_event({
            "total": stats.input_count,
            "recognized": stats.output_count,
            "time_ms": round(stats.processing_time_ms, 2),
        })
        return records, stats


class CommodityClassificationStage(_StageBase):
    event_type = EventType.COMMODITY_CLASSIFIED
    stage_name = "commodity_classification"

    def run(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        stats = self._make_stats()
        stats.input_count = len(records)
        t0 = time.perf_counter()

        for rec in records:
            product = (rec.get("product") or "").lower().strip()
            if product in VALID_PRODUCTS:
                rec["commodity_category"] = product
                continue

            searchable = " ".join(
                str(v) for v in rec.values() if isinstance(v, str)
            ).lower()

            best_category = "grains"
            best_score = 0
            for category, keywords in COMMODITY_KEYWORDS.items():
                score = sum(1 for kw in keywords if kw in searchable)
                if score > best_score:
                    best_score = score
                    best_category = category

            rec["commodity_category"] = best_category

        stats.output_count = len(records)
        stats.processing_time_ms = (time.perf_counter() - t0) * 1000
        self._emit_event({
            "total": stats.input_count,
            "classified": stats.output_count,
            "time_ms": round(stats.processing_time_ms, 2),
        })
        return records, stats


class SectorClassificationStage(_StageBase):
    event_type = EventType.SECTOR_CLASSIFIED
    stage_name = "sector_classification"

    def run(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        stats = self._make_stats()
        stats.input_count = len(records)
        t0 = time.perf_counter()

        for rec in records:
            entity_type = (rec.get("entity_type") or "").lower().strip()
            if entity_type in VALID_ENTITY_TYPES:
                rec["sector_type"] = entity_type
                continue

            searchable = " ".join(
                str(v) for v in rec.values() if isinstance(v, str)
            ).lower()

            best_sector = "wholesaler"
            best_score = 0
            for sector, keywords in SECTOR_KEYWORDS.items():
                score = sum(1 for kw in keywords if kw in searchable)
                if score > best_score:
                    best_score = score
                    best_sector = sector

            rec["sector_type"] = best_sector

        stats.output_count = len(records)
        stats.processing_time_ms = (time.perf_counter() - t0) * 1000
        self._emit_event({
            "total": stats.input_count,
            "classified": stats.output_count,
            "time_ms": round(stats.processing_time_ms, 2),
        })
        return records, stats


# ---------------------------------------------------------------------------
# DataPipeline orchestrator
# ---------------------------------------------------------------------------

class DataPipeline:
    """Chains all eight processing stages and returns a PipelineResult."""

    def __init__(self, *, skip_stages: Optional[List[str]] = None):
        self._skip = set(skip_stages or [])
        self._stages: List[Tuple[str, Callable]] = [
            ("validation", ValidationStage().run),
            ("deduplication", DeduplicationStage().run),
            ("cleaning", CleaningStage().run),
            ("normalization", NormalizationStage().run),
            ("translation", TranslationStage().run),
            ("entity_recognition", EntityRecognitionStage().run),
            ("commodity_classification", CommodityClassificationStage().run),
            ("sector_classification", SectorClassificationStage().run),
        ]

    async def run(self, records: List[Dict[str, Any]]) -> PipelineResult:
        """Execute every stage sequentially, passing output forward.

        Each stage receives a list of dicts and returns ``(list, StageStats)``.
        If a stage is skipped via ``skip_stages`` it is counted but not executed.
        """
        result = PipelineResult(records_in=len(records))
        pipeline_t0 = time.perf_counter()

        await event_bus.emit(
            EventType.CYCLE_START,
            {"pipeline": "data_pipeline", "input_count": len(records)},
            source="pipeline",
        )

        current = records
        for stage_name, stage_fn in self._stages:
            if stage_name in self._skip:
                stats = StageStats(
                    stage_name=stage_name,
                    input_count=len(current),
                    output_count=len(current),
                )
                result.stages.append(stats)
                logger.info(f"Pipeline stage '{stage_name}' skipped")
                continue

            try:
                current, stats = stage_fn(current)
                result.stages.append(stats)
                logger.info(
                    f"Pipeline stage '{stage_name}': "
                    f"{stats.input_count} → {stats.output_count} "
                    f"({stats.processing_time_ms:.1f}ms, "
                    f"{stats.errors} errors)"
                )
            except Exception as exc:
                stats = StageStats(
                    stage_name=stage_name,
                    input_count=len(current),
                    output_count=len(current),
                    errors=1,
                    error_details=[str(exc)],
                )
                result.stages.append(stats)
                result.errors.append(f"{stage_name}: {exc}")
                logger.error(f"Pipeline stage '{stage_name}' failed: {exc}")

        result.records_out = len(current)
        result.total_processing_time_ms = (time.perf_counter() - pipeline_t0) * 1000
        result.succeeded = not result.errors

        await event_bus.emit(
            EventType.CYCLE_COMPLETE,
            {
                "pipeline": "data_pipeline",
                "records_out": result.records_out,
                "total_time_ms": round(result.total_processing_time_ms, 2),
                "stages_run": len(result.stages),
                "errors": len(result.errors),
            },
            source="pipeline",
        )

        logger.info(
            f"Pipeline complete: {result.records_in} → {result.records_out} "
            f"records in {result.total_processing_time_ms:.1f}ms"
        )
        return result
