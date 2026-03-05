from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from copy import copy, deepcopy
from string.templatelib import Interpolation, Template
from typing import ClassVar, Final, Literal, Self, final, override
from enum import Enum, auto, unique


class _Join:
    @override
    def __init__(self, sep: str, first_sep: str = '') -> None:
        self.first = True
        self.first_sep = first_sep
        self.sep = sep

    def __call__(self) -> str:
        if self.first:
            sep = self.first_sep
            self.first = False
        else:
            sep = self.sep
        return sep


def _newline_comma(indent: str = ' ' * 4, sep: str = ',') -> _Join:
    return _Join(f'{sep}{indent[len(sep) :]}', first_sep=indent)


def _find_index[T](values: Iterable[T], predicate: Callable[[T], bool]) -> int:
    for index, value in enumerate(values):
        if predicate(value):
            return index
    raise ValueError('predicate did not return True for any value')


type ColumnType = Literal['INTEGER', 'TEXT']


@final
@unique
class Placeholder(Enum):
    NAME = auto()


NAME: Final = Placeholder.NAME


@final
class Column:
    _DEFAULT_NULLABLE: ClassVar = False
    _DEFAULT_UNIQUE: ClassVar = False
    _DEFAULT_PRIMARY_KEY: ClassVar = False
    _DEFAULT_AUTOINCREMENT: ClassVar = False

    @override
    def __init__(
        self,
        name: str,
        type: ColumnType,
        *,
        nullable: bool | None = None,
        unique: bool | None = None,
        primary_key: bool | None = None,
        autoincrement: bool | None = None,
        check: Template | None = None,
    ) -> None:
        if nullable is None:
            nullable = self._DEFAULT_NULLABLE
        if unique is None:
            unique = self._DEFAULT_UNIQUE
        if primary_key is None:
            primary_key = self._DEFAULT_PRIMARY_KEY
        if autoincrement is None:
            autoincrement = self._DEFAULT_AUTOINCREMENT
        self.name: str = name
        self.type: ColumnType = type
        self.nullable: bool = nullable
        self.unique: bool = unique
        self.primary_key: bool = primary_key
        self.autoincrement: bool = autoincrement
        self.check: Template | None = check

    @override
    def __repr__(self) -> str:
        frags = [f'{type(self).__name__}({self.name!r}, {self.type!r}']  # )
        if self.nullable != self._DEFAULT_NULLABLE:
            frags.append(f', nullable={self.nullable}')
        if self.unique != self._DEFAULT_UNIQUE:
            frags.append(f', unique={self.unique}')
        if self.primary_key != self._DEFAULT_PRIMARY_KEY:
            frags.append(f', primary_key={self.primary_key}')
        if self.autoincrement != self._DEFAULT_AUTOINCREMENT:
            frags.append(f', autoincrement={self.autoincrement}')
        if self.check is not None:
            frags.append(f', check={self.check}')
        frags.append(')')
        return ''.join(frags)

    @override
    def __str__(self) -> str:
        frags = [self.name, ' ', self.type]
        if not self.nullable:
            frags.append(' NOT NULL')
        if self.unique:
            frags.append(' UNIQUE')
        if self.primary_key:
            frags.append(' PRIMARY KEY')
        if self.autoincrement:
            frags.append(' AUTOINCREMENT')
        if self.check is not None:
            frags.append(' CHECK (')  # )
            for item in self.check:
                match item:
                    case str() as text:
                        pass
                    case Interpolation(Placeholder.NAME):
                        text = self.name
                    case _:
                        raise ValueError()
                frags.append(text)
            frags.append(')')
        return ''.join(frags)

    def __copy__(self) -> Self:
        return type(self)(
            self.name,
            self.type,
            nullable=self.nullable,
            unique=self.unique,
            primary_key=self.primary_key,
            autoincrement=self.autoincrement,
            check=self.check,
        )

    def __deepcopy__(self, memo: object) -> Self:
        _ = memo
        return copy(self)


@final
class Table:
    @override
    def __init__(
        self,
        name: str,
        columns: Iterable[Column] | None = None,
        primary_key: Iterable[Column | str] | None = None,
        unique_constraints: Iterable[Iterable[Column]] | None = None,
        check_constraints: Iterable[Template] | None = None,
    ) -> None:
        if columns is None:
            columns = ()
        if unique_constraints is None:
            unique_constraints = ()
        if check_constraints is None:
            check_constraints = ()
        self.name = name
        self.columns: list[Column] = list(columns)
        self._primary_key: list[Column] | None = None
        self.unique_constraints: list[list[Column]] = [
            list(columns) for columns in unique_constraints
        ]
        self.check_constraints: list[Template] = list(check_constraints)

        self.primary_key = primary_key

    @override
    def __str__(self) -> str:
        frags = ['CREATE TABLE ', self.name, ' (\n']  # )
        join = _newline_comma()
        for column in self.columns:
            frags.append(join())
            frags.append(str(column))
            frags.append('\n')
        for constraint in self.unique_constraints:
            frags.append(join())
            frags.append('UNIQUE (')  # )
            frags.append(', '.join(column.name for column in constraint))
            frags.append(')\n')
        for constraint in self.check_constraints:
            frags.append(join())
            frags.append('CHECK (')  # )
            for item in constraint:
                match item:
                    case str() as frag:
                        pass
                    case Interpolation(Column(name=frag)):
                        pass
                    case _:
                        raise ValueError(
                            f'invalid table check contraints template item {item!r}'
                        )
                frags.append(frag)
            frags.append(')\n')
        frags.append(') STRICT')
        return ''.join(frags)

    def __copy__(self) -> Self:
        return type(self)(
            self.name,
            columns=self.columns,
            primary_key=self._primary_key,
            unique_constraints=self.unique_constraints,
            check_constraints=self.check_constraints,
        )

    def __deepcopy__(self, memo: dict[int, object]) -> Self:
        columns = {
            column.name: deepcopy(column, memo) for column in self.columns
        }

        if self._primary_key is None:
            primary_key = None
        else:
            primary_key = (columns[column.name] for column in self._primary_key)

        return type(self)(
            self.name,
            columns=columns.values(),
            primary_key=primary_key,
            unique_constraints=(
                (columns[column.name] for column in unique_constraint)
                for unique_constraint in self.unique_constraints
            ),
            check_constraints=(
                Template(
                    *(
                        Interpolation(
                            columns[item.value.name],
                            item.expression,
                            item.conversion,
                            item.format_spec,
                        )
                        if isinstance(item, Interpolation)
                        and isinstance(item.value, Column)
                        else item
                        for item in check_constraint
                    )
                )
                for check_constraint in self.check_constraints
            ),
        )

    def __getitem__(self, key: str, /) -> Column:
        for column in self.columns:
            if column.name == key:
                return column
        raise KeyError(f'no column named {key!r}')

    @property
    def primary_key(self) -> Sequence[Column] | None:
        return self._primary_key

    @primary_key.setter
    def primary_key(self, columns: Iterable[Column | str] | None) -> None:
        if columns is None:
            self._primary_key = None
            return
        if isinstance(columns, (Column, str)):
            columns = (columns,)
        self._primary_key = [
            self[column] if isinstance(column, str) else column
            for column in columns
        ]

    def append_column(self, column: Column) -> None:
        self.columns.append(column)


@final
class Database:
    def __init__(self, tables: Mapping[str, Table] | None = None) -> None:
        if tables is None:
            tables = {}
        else:
            tables = dict(tables)

        self.tables: dict[str, Table] = tables

    def __copy__(self) -> Self:
        return type(self)(tables=self.tables)

    def __deepcopy__(self, memo: dict[int, object]) -> Self:
        return type(self)(tables=deepcopy(self.tables, memo))

    def set_table(self, table: Table | AlterTable) -> Table:
        if isinstance(table, AlterTable):
            table = table.table_after()
        self.tables[table.name] = table
        return table


@final
@unique
class _Default(Enum):
    DEFAULT = auto()


DEFAULT: Final = _Default.DEFAULT


@final
class AlterTable:
    @override
    def __init__(self, table: Table) -> None:
        self.table = table
        self.temp_table = deepcopy(table)
        self.temp_table.name = f'next_{self.table.name}'
        self._inits: dict[str, Literal[_Default.DEFAULT] | str] = {}

    @override
    def __str__(self) -> str:
        return ';\n\n'.join(self.statements)

    def _insert(self) -> str:
        frags = [f'INSERT INTO {self.temp_table.name} (\n']  # )
        prefix = _newline_comma()
        for column in self.temp_table.columns:
            if self._inits.get(column.name) is DEFAULT:
                continue
            frags.append(prefix())
            frags.append(column.name)
            frags.append('\n')
        frags.append(')\nSELECT\n')
        prefix = _newline_comma()
        for column in self.temp_table.columns:
            init = self._inits.get(column.name, column.name)
            if init is DEFAULT:
                continue
            frags.append(prefix())
            frags.append(init or column.name)
            frags.append('\n')
        frags.append(f'FROM {self.table.name}')
        return ''.join(frags)

    @property
    def statements(self) -> Iterator[str]:
        yield str(self.temp_table)
        yield self._insert()
        yield f'DROP TABLE {self.table.name}'
        yield f'ALTER TABLE {self.temp_table.name} RENAME TO {self.table.name}'

    def _insert_column(
        self,
        existing_column: str,
        index_offset: int,
        new_column: Column,
        init: Literal[_Default.DEFAULT] | str,
    ) -> None:
        index = (
            _find_index(
                self.temp_table.columns,
                lambda c: c.name == existing_column,
            )
            + index_offset
        )
        self.temp_table.columns.insert(index, new_column)
        self._inits[new_column.name] = init

    def insert_column_before(
        self,
        existing_column: str,
        new_column: Column,
        init: Literal[_Default.DEFAULT] | str,
    ) -> Column:
        self._insert_column(existing_column, 0, new_column, init)
        return new_column

    def insert_column_after(
        self,
        existing_column: str,
        new_column: Column,
        init: Literal[_Default.DEFAULT] | str,
    ) -> Column:
        self._insert_column(existing_column, 1, new_column, init)
        return new_column

    def drop_column(self, column: Column | str) -> None:
        if isinstance(column, str):
            column = self.temp_table[column]
        self.temp_table.columns.remove(column)

    def drop_primary_key(self) -> None:
        self.temp_table.primary_key = None

    def append_unique_constraint(self, columns: Iterable[Column | str]) -> None:
        self.temp_table.unique_constraints.append(
            [
                self.temp_table[column] if isinstance(column, str) else column
                for column in columns
            ]
        )

    def append_check_constraint(self, expression: Template) -> None:
        self.temp_table.check_constraints.append(expression)

    def table_after(self) -> Table:
        table = deepcopy(self.temp_table)
        table.name = self.table.name
        return table


@final
class Step:
    @override
    def __init__(self) -> None:
        self._steps: Final[list[AlterTable | Table | str]] = []

    @override
    def __str__(self) -> str:
        return ';\n\n'.join(self.statements)

    def __call__[T: (AlterTable, Table, str)](self, step: T) -> T:
        self._steps.append(step)
        return step

    @property
    def statements(self) -> Iterator[str]:
        yield 'PRAGMA foreign_keys=OFF'
        yield 'BEGIN'
        for step in self._steps:
            match step:
                case AlterTable():
                    yield from step.statements
                case Table():
                    yield str(step)
                case str():
                    yield step
        yield 'PRAGMA foreign_key_check'
        yield 'COMMIT'
        yield 'PRAGMA foreign_keys=ON'
