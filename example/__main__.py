from typing import Final

from litegrate import AlterTable, Column, DEFAULT, Database, NAME, Step, Table

from example.helpers import (
    Columns,
    bool_column,
    id_column,
    integer_column,
    text_column,
    timestamp_column,
)

_MIGRATIONS_TABLE: Final = 'migrations'


def _update_verison(step: Step, version: int) -> None:
    step(f'UPDATE migrations SET version = {version}')


def _migrate_1(db: Database, step: Step) -> None:
    # fmt: off
    Columns(db.set_table(step(Table(_MIGRATIONS_TABLE)))) \
        .id(check=t'{NAME} = 1') \
        .integer('version', check=t'{NAME} >= 1')
    # fmt: on

    # fmt: off
    Columns(db.set_table(step(Table('applications')))) \
        .text('site', unique=True) \
        .text('reference') \
        .text('summary') \
        .text('status') \
        .timestamp('status_updated') \
        .text('address') \
        .text('link') \
        .text('received', nullable=True) \
        .text('validated', nullable=True) \
        .timestamp('inserted') \
        .table \
        .primary_key = 'site', 'reference'
    # fmt: on

    step(f'INSERT INTO {_MIGRATIONS_TABLE} (id, version) VALUES (1, 1)')


def _migrate_2(db: Database, step: Step) -> None:
    alter = step(AlterTable(db.tables['applications']))
    alter.insert_column_after(
        'address',
        text_column('address_formatted', nullable=True),
        'NULL',
    )
    db.set_table(alter)
    _update_verison(step, 2)


def _migrate_3(db: Database, step: Step) -> None:
    alter = step(AlterTable(db.tables['applications']))
    alter.insert_column_after(
        'inserted',
        timestamp_column('updated', nullable=True),
        'NULL',
    )
    db.set_table(alter)
    _update_verison(step, 3)


def _migrate_4(db: Database, step: Step) -> None:
    alter = step(AlterTable(db.tables['applications']))
    alter.insert_column_after(
        'updated',
        Column(
            'action',
            'TEXT',
            nullable=True,
            check=t"{NAME} = 'ignore' OR {NAME} = 'mail'",
        ),
        'NULL',
    )
    db.set_table(alter)
    _update_verison(step, 5)


def _v5_indexes(step: Step) -> None:
    step('CREATE INDEX applications_action ON applications (action)')
    step('CREATE INDEX applications_address ON applications (address)')
    step(r"""
        CREATE UNIQUE INDEX application_table_sort ON applications (
            COALESCE(validated, received, updated, inserted) DESC
        ,   site
        ,   reference
        )
    """)


def _migrate_5(db: Database, step: Step) -> None:
    alter = step(AlterTable(db.tables['applications']))
    alter.insert_column_before('site', id_column(), DEFAULT)
    alter.insert_column_after(
        'summary',
        text_column('summary_edited', nullable=True),
        'NULL',
    )
    alter.insert_column_after(
        'address_formatted',
        text_column('address_edited', nullable=True),
        'NULL',
    )
    alter.insert_column_after(
        'updated',
        timestamp_column('letter_created_at', nullable=True),
        'NULL',
    )
    alter.append_unique_constraint(('site', 'reference'))
    alter.append_check_constraint(t'summary != summary_edited')
    db.set_table(alter.table_after())
    _v5_indexes(step)
    _update_verison(step, 5)


def _migrate_6(db: Database, step: Step) -> None:
    alter = step(AlterTable(db.tables['applications']))
    alter.insert_column_after('summary', bool_column('summary_ignore'), 'FALSE')
    db.set_table(alter.table_after())
    _v5_indexes(step)
    _update_verison(step, 6)


def _migrate_7(db: Database, step: Step) -> None:
    alter = step(AlterTable(db.tables['applications']))
    alter.insert_column_after('address', bool_column('address_ignore'), 'FALSE')
    alter.insert_column_after(
        'address_formatted', bool_column('address_formatted_ignore'), 'FALSE'
    )
    alter.append_check_constraint(
        t'NOT (address_formatted IS NULL AND address_formatted_ignore))'
    )
    db.set_table(alter.table_after())
    _v5_indexes(step)
    _update_verison(step, 7)


def _migrate_8(db: Database, step: Step) -> None:
    alter = step(AlterTable(db.tables['applications']))
    alter.insert_column_after(
        'status',
        integer_column('status_kind', check=t'{NAME} BETWEEN 0 AND 4'),
        '4 -- Error',
    )
    db.set_table(alter.table_after())
    _v5_indexes(step)
    _update_verison(step, 8)


def main() -> None:
    db = Database()
    migrations = [
        _migrate_1,
        _migrate_2,
        _migrate_3,
        _migrate_4,
        _migrate_5,
        _migrate_6,
        _migrate_7,
        _migrate_8,
    ]
    for migration in migrations:
        step = Step()
        migration(db, step)
        print(step)
        input()


if __name__ == '__main__':
    main()
