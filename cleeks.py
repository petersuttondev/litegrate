# Imports →
from pathlib import Path
from typing import TYPE_CHECKING

from cleek import task

if TYPE_CHECKING:
    from collections.abc import Iterable
    from subprocess import CompletedProcess

# ←


@task
def clean(
    dry_run: bool = False,
    autoenv: bool = False,
) -> None:
    args = ['git', 'clean', '--force']
    if dry_run:
        args.append('--dry-run')

    def exclude(pattern: str) -> None:
        args.append(f'--exclude=!{pattern}')

    if not autoenv:
        exclude('.autoenv*')

    args.append('-X')
    args.append('.')
    _run(args)


@task
def venv() -> None:
    _run('python', '-m', 'venv', 'venv')


@task
def install() -> None:
    _run(
        'venv/bin/pip',
        'install',
        ('--config-settings', 'editable_mode=strict'),
        ('--group', 'dev'),
        ('--editable', '.'),
    )


@task
def uninstall() -> None:
    _run('venv/bin/pip', 'uninstall', '--yes', 'litegrate')


# Utilities →

type _Args = Iterable[str] | str


def _args_inplace(args: Iterable[_Args], flat: list[str]) -> None:
    for arg in args:
        match arg:
            case str():
                flat.append(arg)
            case _:
                _args_inplace(arg, flat)


def _args(*args: _Args) -> list[str]:
    flat: list[str] = []
    _args_inplace(args, flat)
    return flat


_project_dir: Path | None = None


def _get_project_dir() -> Path:
    global _project_dir
    if _project_dir is None:
        from pathlib import Path

        _project_dir = Path(__file__).resolve(strict=True).parent
    return _project_dir


def _run(*args: _Args) -> CompletedProcess[bytes]:
    from subprocess import CalledProcessError, run

    try:
        return run(_args(*args), cwd=_get_project_dir(), check=True)
    except CalledProcessError as error:
        raise SystemExit(error.returncode)


# ←

# vim:foldmethod=marker:foldmarker=→,←
