"Useful AST classes that don't quite fall within the scope of regular SQL"

from collections.abc import Callable, Sequence

import attrs

from data_diff.abcs.database_types import ColType
from data_diff.queries.ast_classes import Expr, ExprNode


@attrs.define(frozen=True)
class NormalizeAsString(ExprNode):
    expr: ExprNode
    expr_type: ColType | None = None

    @property
    def type(self) -> type | None:
        return str


@attrs.define(frozen=True)
class ApplyFuncAndNormalizeAsString(ExprNode):
    expr: ExprNode
    apply_func: Callable | None = None


@attrs.define(frozen=True)
class Checksum(ExprNode):
    exprs: Sequence[Expr]
