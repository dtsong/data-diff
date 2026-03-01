from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class ManifestJsonConfig(BaseModel):
    class Metadata(BaseModel):
        dbt_version: str = Field(..., pattern=r"^\d+\.\d+\.\d+([a-zA-Z0-9]+)?$")
        project_id: str | None
        user_id: str | None

    class Nodes(BaseModel):
        class Config(BaseModel):
            database: str | None
            schema_: str | None = Field(..., alias="schema")
            tags: list[str]

        class Column(BaseModel):
            meta: dict[str, Any]
            tags: list[str]

        class TestMetadata(BaseModel):
            name: str
            kwargs: dict[str, Any]

        class DependsOn(BaseModel):
            macros: list[str] = []
            nodes: list[str] = []

        unique_id: str
        resource_type: str
        name: str
        alias: str
        database: str | None
        schema_: str = Field(..., alias="schema")
        columns: dict[str, Column] | None
        meta: dict[str, Any]
        config: Config
        tags: list[str]
        test_metadata: TestMetadata | None
        depends_on: DependsOn

    metadata: Metadata
    nodes: dict[str, Nodes]


class RunResultsJsonConfig(BaseModel):
    class Metadata(BaseModel):
        dbt_version: str = Field(..., pattern=r"^\d+\.\d+\.\d+([a-zA-Z0-9]+)?$")

    class Results(BaseModel):
        class Status(Enum):
            success = "success"
            error = "error"
            skipped = "skipped"
            pass_ = "pass"
            fail = "fail"
            warn = "warn"
            runtime_error = "runtime error"

        status: Status
        unique_id: str = Field("...")

    metadata: Metadata
    results: list[Results]
