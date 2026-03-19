"""
queries.py — Query generator for Hansard Search load tests.

Buckets:
  easy    — single common term, low FTS cost, cache-friendly
  medium  — two-term OR, moderate cost
  heavy   — compound AND/OR, higher cost
  worst   — 6-term OR + high page + case_sensitive, always unfriendly

Filter variants: ~20% of requests add a random party or date_from filter.
"""

import random
from typing import Optional

# ~200 parliamentary vocabulary terms
_VOCAB = [
    "water", "climate", "budget", "health", "education", "defence", "housing",
    "energy", "immigration", "taxation", "trade", "agriculture", "infrastructure",
    "employment", "superannuation", "Medicare", "childcare", "environment",
    "transport", "telecommunications", "finance", "justice", "security",
    "economy", "parliament", "legislation", "regulation", "policy", "reform",
    "minister", "government", "opposition", "amendment", "bill", "committee",
    "inquiry", "report", "review", "funding", "grants", "spending", "revenue",
    "debt", "deficit", "surplus", "growth", "inflation", "interest", "rates",
    "investment", "exports", "imports", "tariff", "border", "customs",
    "refugee", "asylum", "visa", "citizenship", "multicultural", "diversity",
    "indigenous", "Aboriginal", "Torres", "reconciliation", "treaty", "land",
    "native", "title", "heritage", "culture", "language", "community",
    "welfare", "pension", "disability", "aged", "care", "hospital", "GP",
    "mental", "suicide", "drugs", "pharmaceutical", "vaccine", "pandemic",
    "COVID", "quarantine", "biosecurity", "research", "science", "technology",
    "innovation", "digital", "internet", "broadband", "NBN", "satellite",
    "renewable", "solar", "wind", "nuclear", "coal", "gas", "oil", "mining",
    "resources", "royalties", "emissions", "carbon", "temperature", "drought",
    "flood", "bushfire", "disaster", "relief", "resilience", "adaptation",
    "mitigation", "biodiversity", "species", "conservation", "marine", "ocean",
    "fisheries", "forestry", "biosecurity", "biosafety", "pesticides",
    "chemicals", "waste", "recycling", "plastics", "pollution", "water",
    "Murray", "Darling", "basin", "irrigation", "salinity", "groundwater",
    "roads", "rail", "airports", "shipping", "ports", "logistics", "freight",
    "construction", "housing", "affordability", "rental", "mortgage", "property",
    "planning", "zoning", "density", "urban", "regional", "rural", "remote",
    "school", "university", "TAFE", "vocational", "training", "apprentice",
    "literacy", "numeracy", "curriculum", "teachers", "students", "fees",
    "HECS", "research", "grants", "universities", "science", "STEM",
    "police", "courts", "prisons", "sentencing", "parole", "victims",
    "domestic", "violence", "family", "child", "protection", "abuse",
    "corruption", "integrity", "transparency", "accountability", "audit",
    "privacy", "data", "surveillance", "intelligence", "ASIO", "AFP",
    "foreign", "affairs", "diplomacy", "alliance", "treaty", "sanctions",
    "United", "Nations", "Pacific", "Asia", "China", "United", "States",
    "Europe", "trade", "agreement", "partnership", "competition", "subsidy",
    "banking", "regulation", "APRA", "ASIC", "reserve", "monetary",
    "wages", "penalty", "rates", "enterprise", "bargaining", "union",
    "strike", "industrial", "action", "workplace", "safety", "compensation",
    "small", "business", "startup", "innovation", "entrepreneur", "venture",
    "manufacturing", "industry", "steel", "automotive", "textile", "pharmaceutical",
]

_PARTIES = ["ALP", "LP", "NP", "GRN", "ON", "UAP", "IND", "LNP", "CLP"]

_DATE_RANGES = [
    ("2000-01-01", "2005-12-31"),
    ("2005-01-01", "2010-12-31"),
    ("2010-01-01", "2015-12-31"),
    ("2015-01-01", "2020-12-31"),
    ("2020-01-01", "2025-12-31"),
    ("2010-01-01", "2020-12-31"),
    ("2003-01-01", "2007-12-31"),
]

_CHAMBERS = ["senate", "house", "both", "both", "both"]  # weighted toward both

_SORT_COLS = ["date", "date", "date", "relevance"]  # weighted toward date
_SORT_DIRS = ["asc", "desc"]


class QueryGenerator:
    """Generates randomised search requests for load testing."""

    BUCKETS = ("easy", "medium", "heavy", "worst")

    def __init__(
        self,
        weights: tuple[float, float, float, float] = (0.40, 0.30, 0.20, 0.10),
        seed: Optional[int] = None,
    ):
        if len(weights) != 4:
            raise ValueError("weights must have 4 elements: easy medium heavy worst")
        total = sum(weights)
        self.weights = [w / total for w in weights]
        self._rng = random.Random(seed)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def next_request(self, bucket: Optional[str] = None) -> dict:
        """Return a dict ready to POST to /api/search."""
        if bucket is None:
            bucket = self._rng.choices(self.BUCKETS, weights=self.weights, k=1)[0]
        if bucket not in self.BUCKETS:
            raise ValueError(f"Unknown bucket: {bucket!r}")

        builder = getattr(self, f"_build_{bucket}")
        payload = builder()
        payload["bucket"] = bucket  # extra field; server ignores unknown keys
        return payload

    # ------------------------------------------------------------------
    # Bucket builders
    # ------------------------------------------------------------------

    def _build_easy(self) -> dict:
        term = self._rng.choice(_VOCAB)
        return {
            "expression": term,
            "chamber": self._rng.choice(_CHAMBERS),
            "page": 1,
            "page_size": 20,
            "filters": self._maybe_filter(prob=0.15),
            "case_sensitive": False,
            "sort_col": "date",
            "sort_dir": "desc",
        }

    def _build_medium(self) -> dict:
        t1, t2 = self._rng.sample(_VOCAB, 2)
        op = self._rng.choice(["OR", "AND"])
        return {
            "expression": f"{t1} {op} {t2}",
            "chamber": self._rng.choice(_CHAMBERS),
            "page": self._rng.randint(1, 3),
            "page_size": 20,
            "filters": self._maybe_filter(prob=0.20),
            "case_sensitive": False,
            "sort_col": self._rng.choice(_SORT_COLS),
            "sort_dir": self._rng.choice(_SORT_DIRS),
        }

    def _build_heavy(self) -> dict:
        terms = self._rng.sample(_VOCAB, 4)
        t1, t2, t3, t4 = terms
        expression = f"({t1} OR {t2}) AND ({t3} OR {t4})"
        return {
            "expression": expression,
            "chamber": self._rng.choice(_CHAMBERS),
            "page": self._rng.randint(1, 10),
            "page_size": 20,
            "filters": self._maybe_filter(prob=0.25),
            "case_sensitive": False,
            "sort_col": self._rng.choice(_SORT_COLS),
            "sort_dir": self._rng.choice(_SORT_DIRS),
        }

    def _build_worst(self) -> dict:
        terms = self._rng.sample(_VOCAB, 6)
        expression = " OR ".join(terms)
        return {
            "expression": expression,
            "chamber": "both",
            "page": self._rng.randint(10, 30),
            "page_size": 20,
            "filters": self._maybe_filter(prob=0.30),
            "case_sensitive": True,  # always case-sensitive for worst
            "sort_col": self._rng.choice(_SORT_COLS),
            "sort_dir": self._rng.choice(_SORT_DIRS),
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _maybe_filter(self, prob: float = 0.20) -> dict:
        filters: dict = {}
        if self._rng.random() < prob:
            filters["party"] = [self._rng.choice(_PARTIES)]
        if self._rng.random() < prob:
            date_from, date_to = self._rng.choice(_DATE_RANGES)
            filters["date_from"] = date_from
            filters["date_to"] = date_to
        return filters


def make_day_context_request(rng: random.Random) -> dict:
    """Generate a /api/day_context payload."""
    # Random date between 1998 and 2025
    year = rng.randint(1998, 2025)
    month = rng.randint(1, 12)
    day = rng.randint(1, 28)
    chamber = rng.choice(["senate", "house"])
    return {
        "date": f"{year:04d}-{month:02d}-{day:02d}",
        "chamber": chamber,
    }
