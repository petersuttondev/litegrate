from string.templatelib import Template
from typing import Final, Self

from litegrate import Column, NAME, Table

check_non_empty_text: Final = t"{NAME} != ''"

check_bool_like: Final = t'{NAME} == FALSE OR {NAME} == TRUE'


def bool_column(name: str) -> Column:
    return integer_column(name, check=check_bool_like)


def id_column(check: Template | None = None) -> Column:
    if check is None:
        check = t'{NAME} > 0'
    return integer_column(
        'id',
        primary_key=True,
        autoincrement=True,
        check=check,
    )


def integer_column(
    name: str,
    primary_key: bool | None = None,
    autoincrement: bool | None = None,
    check: Template | None = None,
) -> Column:
    return Column(
        name,
        'INTEGER',
        primary_key=primary_key,
        autoincrement=autoincrement,
        check=check,
    )


def text_column(
    name: str,
    nullable: bool | None = None,
    unique: bool | None = None,
    check: Template | None = None,
) -> Column:
    if check is None:
        check = check_non_empty_text
    return Column(name, 'TEXT', nullable=nullable, unique=unique, check=check)


def timestamp_column(name: str, nullable: bool | None = None) -> Column:
    return Column(name, 'INTEGER', nullable=nullable, check=t'{NAME} >= 0')


class Columns:
    def __init__(self, table: Table) -> None:
        self.table = table

    def append(self, column: Column) -> Self:
        self.table.append_column(column)
        return self

    def bool(self, name: str) -> Self:
        return self.append(bool_column(name))

    def id(self, check: Template | None = None) -> Self:
        return self.append(id_column(check=check))

    def integer(
        self,
        name: str,
        primary_key: bool | None = None,
        autoincrement: bool | None = None,
        check: Template | None = None,
    ) -> Self:
        return self.append(
            integer_column(
                name,
                primary_key=primary_key,
                autoincrement=autoincrement,
                check=check,
            )
        )

    def text(
        self,
        name: str,
        nullable: bool | None = None,
        unique: bool | None = None,
        check: Template | None = None,
    ) -> Self:
        return self.append(
            text_column(name, nullable=nullable, unique=unique, check=check)
        )

    def timestamp(self, name: str, nullable: bool | None = None) -> Self:
        return self.append(timestamp_column(name, nullable=nullable))
