"""Tests for Collation comparison logic in data_diff.abcs.database_types."""

import itertools

from data_diff.abcs.database_types import Collation

# --- Equality ---


class TestCollationEquality:
    def test_ordinal_same_language(self):
        a = Collation(ordinal=True, language="Albanian")
        b = Collation(ordinal=True, language="Albanian")
        assert a == b

    def test_ordinal_different_language(self):
        a = Collation(ordinal=True, language="Albanian")
        b = Collation(ordinal=True, language="Latin")
        assert a != b

    def test_ordinal_none_language_treated_equal(self):
        a = Collation(ordinal=True, language=None)
        b = Collation(ordinal=True, language="Latin")
        assert a == b

    def test_ordinal_both_none_language(self):
        a = Collation(ordinal=True)
        b = Collation(ordinal=True)
        assert a == b

    def test_ordinal_vs_non_ordinal(self):
        a = Collation(ordinal=True)
        b = Collation(ordinal=False, language="en")
        assert a != b

    def test_locale_matching(self):
        a = Collation(ordinal=False, language="en", case_sensitive=True, accent_sensitive=True)
        b = Collation(ordinal=False, language="en", case_sensitive=True, accent_sensitive=True)
        assert a == b

    def test_locale_language_mismatch(self):
        a = Collation(ordinal=False, language="en")
        b = Collation(ordinal=False, language="de")
        assert a != b

    def test_country_none_tolerance(self):
        a = Collation(ordinal=False, language="en", country="US")
        b = Collation(ordinal=False, language="en", country=None)
        assert a == b

    def test_country_mismatch(self):
        a = Collation(ordinal=False, language="en", country="US")
        b = Collation(ordinal=False, language="en", country="GB")
        assert a != b

    def test_sensitivity_difference(self):
        a = Collation(ordinal=False, language="en", case_sensitive=True)
        b = Collation(ordinal=False, language="en", case_sensitive=False)
        assert a != b

    def test_not_implemented_for_non_collation(self):
        c = Collation()
        assert c.__eq__("not a collation") is NotImplemented


# --- Ordering ---


class TestCollationOrdering:
    def test_absorbs_damage_precedence(self):
        """absorbs_damage=True makes a collation "lesser" (preferred target) when not otherwise equal."""
        snowflake = Collation(absorbs_damage=True, ordinal=False, language="en")
        regular = Collation(absorbs_damage=False, ordinal=False, language="de")
        # absorbs_damage overrides language ordering: snowflake is always lesser
        assert regular > snowflake
        assert not snowflake > regular

    def test_absorbs_damage_differs_not_equal(self):
        """Collations with different absorbs_damage are not equal."""
        a = Collation(absorbs_damage=True, ordinal=True)
        b = Collation(absorbs_damage=False, ordinal=True)
        assert a != b
        # The non-absorbing side is "greater" (preferred to absorb)
        assert b > a

    def test_ordinal_gt_non_ordinal(self):
        ordinal = Collation(ordinal=True)
        locale = Collation(ordinal=False, language="en")
        assert ordinal > locale

    def test_non_ordinal_lt_ordinal(self):
        ordinal = Collation(ordinal=True)
        locale = Collation(ordinal=False, language="en")
        assert locale < ordinal

    def test_language_ordering(self):
        a = Collation(ordinal=False, language="de")
        b = Collation(ordinal=False, language="en")
        assert b > a  # "en" > "de"

    def test_country_ordering(self):
        a = Collation(ordinal=False, language="en", country="GB")
        b = Collation(ordinal=False, language="en", country="US")
        assert b > a  # "US" > "GB"

    def test_sensitivity_tiebreaker(self):
        a = Collation(ordinal=False, language="en", case_sensitive=False)
        b = Collation(ordinal=False, language="en", case_sensitive=True)
        assert b > a  # True > False

    def test_equal_collations_not_gt(self):
        a = Collation(ordinal=True, language="Latin")
        b = Collation(ordinal=True, language="Latin")
        assert not a > b
        assert not b > a

    def test_gt_not_implemented_for_non_collation(self):
        c = Collation()
        assert c.__gt__("not a collation") is NotImplemented


# --- Total ordering ---


class TestCollationTotalOrdering:
    """Verify no incomparable pairs exist across a diverse set of collations."""

    DIVERSE_COLLATIONS = [
        Collation(),
        Collation(ordinal=True),
        Collation(ordinal=True, language="Albanian"),
        Collation(ordinal=True, language="Latin"),
        Collation(ordinal=False, language="en"),
        Collation(ordinal=False, language="en", country="US"),
        Collation(ordinal=False, language="en", country="GB"),
        Collation(ordinal=False, language="de"),
        Collation(ordinal=False, language="en", case_sensitive=True),
        Collation(ordinal=False, language="en", case_sensitive=False),
        Collation(ordinal=False, language="en", accent_sensitive=True),
        Collation(absorbs_damage=True, ordinal=True),
        Collation(absorbs_damage=True, ordinal=False, language="en"),
        Collation(ordinal=False, language="en", lower_first=True),
    ]

    def test_no_incomparable_pairs(self):
        """For every pair, at least one of ==, >, < must hold."""
        for a, b in itertools.combinations(self.DIVERSE_COLLATIONS, 2):
            comparable = (a == b) or (a > b) or (a < b)
            assert comparable, f"Incomparable pair: {a!r} vs {b!r}"

    def test_reflexive(self):
        for c in self.DIVERSE_COLLATIONS:
            assert c == c

    def test_antisymmetric(self):
        """If a > b then not b > a."""
        for a, b in itertools.combinations(self.DIVERSE_COLLATIONS, 2):
            if a > b:
                assert not b > a, f"Antisymmetry violated: {a!r} vs {b!r}"

    def test_transitive_sample(self):
        """Spot-check transitivity on all triples."""
        for a, b, c in itertools.combinations(self.DIVERSE_COLLATIONS, 3):
            if a > b and b > c:
                assert a > c, f"Transitivity violated: {a!r} > {b!r} > {c!r} but not {a!r} > {c!r}"


# --- Derived operators ---


class TestCollationDerivedOperators:
    def test_ne_consistency(self):
        a = Collation(ordinal=True, language="Albanian")
        b = Collation(ordinal=True, language="Latin")
        assert (a != b) is True
        assert (a == b) is False

    def test_ge_consistency(self):
        a = Collation(ordinal=True)
        b = Collation(ordinal=False, language="en")
        assert (a >= b) == (a > b or a == b)

    def test_le_consistency(self):
        a = Collation(ordinal=True)
        b = Collation(ordinal=False, language="en")
        assert (b <= a) == (b < a or b == a)

    def test_ge_equal(self):
        a = Collation(ordinal=True)
        b = Collation(ordinal=True)
        assert a >= b
        assert b >= a

    def test_le_equal(self):
        a = Collation(ordinal=True)
        b = Collation(ordinal=True)
        assert a <= b
        assert b <= a

    def test_ne_not_implemented(self):
        assert Collation().__ne__("x") is NotImplemented

    def test_ge_not_implemented(self):
        assert Collation().__ge__("x") is NotImplemented

    def test_le_not_implemented(self):
        assert Collation().__le__("x") is NotImplemented

    def test_lt_not_implemented(self):
        assert Collation().__lt__("x") is NotImplemented
