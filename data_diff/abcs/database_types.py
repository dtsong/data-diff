import decimal
from abc import ABC, abstractmethod
from collections.abc import Collection
from datetime import datetime
from typing import TypeVar

import attrs

from data_diff.utils import ArithAlphanumeric, ArithUUID, Unknown

DbPath = tuple[str, ...]
DbKey = int | str | bytes | ArithUUID | ArithAlphanumeric
DbTime = datetime

N = TypeVar("N")


@attrs.frozen(kw_only=True, eq=False, order=False, hash=False)
class Collation:
    """
    A pre-parsed or pre-known record about db collation, per column.

    The "greater" collation should be used as a target collation for textual PKs
    on both sides of the diff — by converting the "lesser" collation to self.

    Snowflake easily absorbs the performance losses, so it has a boost to always
    be greater than any other collation in non-Snowflake databases.
    Other databases need to negotiate which side absorbs the performance impact.
    """

    # A boost for special databases that are known to absorb the performance damage well.
    absorbs_damage: bool = False

    # Ordinal sorting by ASCII/UTF8 (True), or alphabetic as per locale/country/etc (False).
    ordinal: bool | None = None

    # Lowercase first (aAbBcC or abcABC). Otherwise, uppercase first (AaBbCc or ABCabc).
    lower_first: bool | None = None

    # 2-letter lower-case locale and upper-case country codes, e.g. en_US. Ignored for ordinals.
    language: str | None = None
    country: str | None = None

    # There are also space-, punctuation-, width-, kana-(in)sensitivity, so on.
    # Ignore everything not related to xdb alignment. Only case- & accent-sensitivity are common.
    case_sensitive: bool | None = None
    accent_sensitive: bool | None = None

    # Purely informational, for debugging:
    _source: None | str | Collection[str] = None

    def _comparison_key(self) -> tuple:
        """Key for equality and hashing — keeps __eq__/__hash__ consistent."""
        if self.ordinal:
            # Ordinal sorting is by code point; sensitivity flags are irrelevant.
            return (self.absorbs_damage, True, self.language)
        return (
            self.absorbs_damage,
            self.ordinal,  # None vs False are semantically distinct
            self.language,
            self.country,
            self.case_sensitive,
            self.accent_sensitive,
            self.lower_first,
        )

    def _ordering_key(self) -> tuple:
        """Key for deterministic total ordering. None sorts before any real value."""

        # (0,) for None sorts before (1, value) for any real value.
        def _wrap(v: object) -> tuple:
            return (0,) if v is None else (1, v)

        return (
            self.absorbs_damage,
            _wrap(self.ordinal),
            _wrap(self.language),
            _wrap(self.country),
            _wrap(self.case_sensitive),
            _wrap(self.accent_sensitive),
            _wrap(self.lower_first),
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Collation):
            return NotImplemented
        return self._comparison_key() == other._comparison_key()

    def __hash__(self) -> int:
        return hash(self._comparison_key())

    def __ne__(self, other: object) -> bool:
        if not isinstance(other, Collation):
            return NotImplemented
        return not self.__eq__(other)

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, Collation):
            return NotImplemented
        if self == other:
            return False
        # absorbs_damage=True means this db absorbs conversion cost — it should be the target
        # (i.e. "lesser"), so the non-absorbing side is "greater".
        if self.absorbs_damage and not other.absorbs_damage:
            return False
        if other.absorbs_damage and not self.absorbs_damage:
            return True
        if self.ordinal and not other.ordinal:
            return True
        if other.ordinal and not self.ordinal:
            return False
        # Deterministic tuple ordering: (language, country, sensitivity flags).
        # None sorts distinctly from "" / False via (0,) vs (1, value) wrapping.
        return self._ordering_key() > other._ordering_key()

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, Collation):
            return NotImplemented
        return self == other or self.__gt__(other)

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, Collation):
            return NotImplemented
        return self != other and not self.__gt__(other)

    def __le__(self, other: object) -> bool:
        if not isinstance(other, Collation):
            return NotImplemented
        return self == other or not self.__gt__(other)


@attrs.define(frozen=True, kw_only=True)
class ColType:
    # Arbitrary metadata added and fetched at runtime.
    _notes: list[N] = attrs.field(factory=list, init=False, hash=False, eq=False)

    def add_note(self, note: N) -> None:
        self._notes.append(note)

    def get_note(self, cls: type[N]) -> N | None:
        """Get the latest added note of type ``cls`` or its descendants."""
        for note in reversed(self._notes):
            if isinstance(note, cls):
                return note
        return None

    @property
    def supported(self) -> bool:
        return True


@attrs.define(frozen=True)
class PrecisionType(ColType):
    precision: int
    rounds: bool | Unknown = Unknown


@attrs.define(frozen=True)
class Boolean(ColType):
    precision = 0


@attrs.define(frozen=True)
class TemporalType(PrecisionType):
    pass


@attrs.define(frozen=True)
class Timestamp(TemporalType):
    pass


@attrs.define(frozen=True)
class TimestampTZ(TemporalType):
    pass


@attrs.define(frozen=True)
class Datetime(TemporalType):
    pass


@attrs.define(frozen=True)
class Date(TemporalType):
    pass


@attrs.define(frozen=True)
class Time(TemporalType):
    pass


@attrs.define(frozen=True)
class NumericType(ColType):
    # 'precision' signifies how many fractional digits (after the dot) we want to compare
    precision: int


@attrs.define(frozen=True)
class FractionalType(NumericType):
    pass


@attrs.define(frozen=True)
class Float(FractionalType):
    python_type = float


@attrs.define(frozen=True)
class IKey(ABC):
    "Interface for ColType, for using a column as a key in table."

    @property
    @abstractmethod
    def python_type(self) -> type:
        "Return the equivalent Python type of the key"

    def make_value(self, value):
        if isinstance(value, self.python_type):
            return value
        return self.python_type(value)


@attrs.define(frozen=True)
class Decimal(FractionalType, IKey):  # Snowflake may use Decimal as a key
    @property
    def python_type(self) -> type:
        if self.precision == 0:
            return int
        return decimal.Decimal


@attrs.define(frozen=True)
class StringType(ColType):
    python_type = str
    collation: Collation | None = attrs.field(default=None, kw_only=True)


@attrs.define(frozen=True)
class ColType_UUID(ColType, IKey):
    python_type = ArithUUID


@attrs.define(frozen=True)
class ColType_Alphanum(ColType, IKey):
    python_type = ArithAlphanumeric


@attrs.define(frozen=True)
class Native_UUID(ColType_UUID):
    pass


@attrs.define(frozen=True)
class String_UUID(ColType_UUID, StringType):
    # Case is important for UUIDs stored as regular string, not native UUIDs stored as numbers.
    # We slice them internally as numbers, but render them back to SQL as lower/upper case.
    # None means we do not know for sure, behave as with False, but it might be unreliable.
    lowercase: bool | None = None
    uppercase: bool | None = None

    def make_value(self, v: str) -> ArithUUID:
        return self.python_type(v, lowercase=self.lowercase, uppercase=self.uppercase)


@attrs.define(frozen=True)
class String_Alphanum(ColType_Alphanum, StringType):
    @staticmethod
    def test_value(value: str) -> bool:
        try:
            ArithAlphanumeric(value)
            return True
        except ValueError:
            return False


@attrs.define(frozen=True)
class String_VaryingAlphanum(String_Alphanum):
    pass


@attrs.define(frozen=True)
class String_FixedAlphanum(String_Alphanum):
    length: int

    def make_value(self, value):
        if isinstance(value, self.python_type):
            return value
        if len(value) != self.length:
            raise ValueError(f"Expected alphanumeric value of length {self.length}, but got '{value}'.")
        return self.python_type(value, max_len=self.length)


@attrs.define(frozen=True)
class Text(StringType):
    @property
    def supported(self) -> bool:
        return False


# In majority of DBMSes, it is called JSON/JSONB. Only in Snowflake, it is OBJECT.
@attrs.define(frozen=True)
class JSON(ColType):
    pass


@attrs.define(frozen=True)
class Array(ColType):
    item_type: ColType


# Unlike JSON, structs are not free-form and have a very specific set of fields and their types.
# We do not parse & use those fields now, but we can do this later.
# For example, in BigQuery:
# - https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type
# - https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#struct_literals
@attrs.define(frozen=True)
class Struct(ColType):
    pass


@attrs.define(frozen=True)
class Integer(NumericType, IKey):
    precision: int = 0
    python_type: type = int

    def __attrs_post_init__(self) -> None:
        if self.precision != 0:
            raise ValueError(f"Integer type must have precision=0, got {self.precision!r}.")


@attrs.define(frozen=True)
class UnknownColType(ColType):
    text: str

    @property
    def supported(self) -> bool:
        return False
