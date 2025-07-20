import fnmatch
import getpass
import glob
import shlex
import sys
from argparse import ArgumentParser, ArgumentError
from pathlib import Path, PurePath
from tempfile import TemporaryDirectory
from typing import Sequence, Iterable

import typer
from paramiko.client import WarningPolicy
from paramiko.sftp_attr import SFTPAttributes
from paramiko.sftp_client import SFTPClient
from pydantic import TypeAdapter
from paramiko import SSHClient
from rich.columns import Columns
from rich.console import Console
from rich.progress import Progress

from sftp_repl.completions import (
    is_dir,
    ConsoleInteractor,
    configure_readline,
    readline,
)
from sftp_repl.utils import format_name, long_listing, SftpUrl, handle_io_error

app = typer.Typer()
console = Console()


class ParserError(Exception):
    pass


def parse_args(parser, args: Sequence[str]):
    try:
        args = parser.parse_args(args)
    except ArgumentError as ex:
        console.print(parser.format_usage(), end="")
        console.print(f"[red]{ex.message}[/red]")
        raise ParserError

    if args.help:
        console.print(parser.format_help())
        raise ParserError()

    return args


def search_glob(
    sftp_client: SFTPClient, current_dir: PurePath, glob_parts: Sequence[str]
) -> list[tuple[PurePath, SFTPAttributes]]:
    if not glob_parts:
        return [(current_dir, sftp_client.stat(str(current_dir)))]
    if glob_parts[0] == "..":
        return search_glob(sftp_client, current_dir / "..", glob_parts[1:])

    matching_files = []
    files = sftp_client.listdir_attr(str(current_dir))
    for file in files:
        if fnmatch.fnmatch(file.filename, glob_parts[0]):
            if len(glob_parts) == 1:
                matching_files.append((current_dir / file.filename, file))
            else:
                matching_files.extend(
                    search_glob(
                        sftp_client, current_dir / file.filename, glob_parts[1:]
                    )
                )
    return matching_files


def expand_path_globs(
    paths: Iterable[PurePath], sftp_client: SFTPClient
) -> list[tuple[PurePath, SFTPAttributes]]:
    matching_files = []
    for path in paths:
        matching_files.extend(
            search_glob(sftp_client, PurePath(path.root or "."), path.parts)
        )
    return matching_files


def ls(sftp_client: SFTPClient, *args):
    """List files in the specified directory."""
    parser = ArgumentParser("ls", add_help=False, exit_on_error=False)
    parser.add_argument(
        "paths", nargs="*", default=[PurePath(".")], type=PurePath, help="Path to list"
    )
    parser.add_argument("--help", action="store_true", help="Show")
    parser.add_argument(
        "-l", action="store_true", dest="long", help="Long listing format"
    )
    parser.add_argument(
        "-h", action="store_true", dest="human", help="Human readable sizes"
    )
    args = parse_args(parser, args)

    try:
        matching_files = expand_path_globs(args.paths, sftp_client)
        multi = len(matching_files) > 1
        prev_listing = False
        for path, sftp_attr in sorted(
            matching_files, key=lambda pf: (is_dir(pf[1]), str(pf[0]).lower())
        ):
            if is_dir(sftp_attr):
                attrs = sftp_client.listdir_attr(str(path))
                if multi:
                    files = [(a.filename, a) for a in attrs]
                    console.print(
                        f"{'\n' if prev_listing else ''}[bold cyan]{path}[/bold cyan]:",
                        highlight=False,
                    )
                else:
                    files = [(a.filename, a) for a in attrs]
            else:
                files = [(str(path), sftp_attr)]

            _list_files(files, args.human, args.long)
            prev_listing = True

    except IOError as ex:
        console.print(f"[red]{ex}[/red]")


def _list_files(files: list[tuple[str, SFTPAttributes]], human: bool, long: bool):
    if long:
        for name, file in sorted(
            files,
            key=lambda f: f[0].lower(),
        ):
            console.print(
                long_listing(name, file, human_readable=human),
                highlight=False,
            )
    else:
        formatted_files = [
            format_name(name, f)
            for name, f in sorted(files, key=lambda f: f[0].lower())
        ]
        console.print(Columns(formatted_files))


@handle_io_error(console)
def cd(sftp_client: SFTPClient, *args):
    parser = ArgumentParser("cd", add_help=False, exit_on_error=False)
    parser.add_argument("path", help="Path to change to")
    parser.add_argument("--help", action="store_true", help="Show")
    args = parse_args(parser, args)

    sftp_client.chdir(args.path)


@handle_io_error(console)
def get(sftp_client: SFTPClient, *args):
    parser = ArgumentParser("get", add_help=False, exit_on_error=False)
    parser.add_argument("src", type=Path, help="source path")
    parser.add_argument(
        "dst", type=Path, nargs="?", default=Path("."), help="destination path"
    )
    parser.add_argument("--help", action="store_true", help="Show")
    args = parse_args(parser, args)

    matching_files = expand_path_globs([args.src], sftp_client)

    if not matching_files:
        console.print(f"[red]File {args.src} not found")
        return

    if len(matching_files) > 1 and not args.dst.is_dir():
        console.print(f"[red]{args.dst}: Not a directory")
        return

    for src, src_attrs in matching_files:
        if is_dir(src_attrs):
            console.print(f"[red]{src} is a directory")
            continue

        with Progress() as progress:
            dst_name = args.dst / src.name if args.dst.is_dir() else args.dst
            task = progress.add_task(
                f"[cyan]Fetching {src} to {dst_name}[/cyan]",
            )

            update_called = False

            def _update(current, total):
                # for empty files
                nonlocal update_called
                update_called = True
                if total == 0:
                    total, current = 1, 1
                progress.update(task, completed=current, total=total, refresh=True)

            sftp_client.get(str(src), str(dst_name), callback=_update)

            if not update_called:
                progress.update(task, completed=1, total=1, refresh=True)


def put(sftp_client: SFTPClient, *args):
    parser = ArgumentParser("put", add_help=False, exit_on_error=False)
    parser.add_argument("src", type=str, help="source path")
    parser.add_argument(
        "dst", type=PurePath, nargs="?", default=PurePath("."), help="destination path"
    )
    parser.add_argument("--help", action="store_true", help="Show")
    args = parse_args(parser, args)

    matching_files = glob.glob(args.src)
    if not matching_files:
        console.print(f"[red]File {args.src} not found")
        return

    try:
        dst_attr = sftp_client.stat(str(args.dst))
    except IOError:
        dst_attr = None

    if len(matching_files) > 1 and (dst_attr is None or not is_dir(dst_attr)):
        console.print(f"[red]{args.dst}: Not a directory")
        return

    for src in matching_files:
        src_path = Path(src)
        with Progress() as progress:
            dst_name = (
                args.dst / src_path.name if dst_attr and is_dir(dst_attr) else args.dst
            )
            task = progress.add_task(
                f"[cyan]Uploading {src} to {dst_name}[/cyan]",
            )

            update_called = False

            def _update(current, total):
                # for empty files
                nonlocal update_called
                update_called = True
                if total == 0:
                    total, current = 1, 1
                progress.update(task, completed=current, total=total, refresh=True)

            sftp_client.put(str(src), str(dst_name), callback=_update)

            if not update_called:
                progress.update(task, completed=1, total=1, refresh=True)


@handle_io_error(console)
def rm(sftp_client: SFTPClient, *args):
    parser = ArgumentParser("rm", add_help=False, exit_on_error=False)
    parser.add_argument("paths", nargs="+", type=PurePath, help="Path to list")
    parser.add_argument("--help", action="store_true", help="Show")

    args = parse_args(parser, args)

    matching_files = expand_path_globs(args.paths, sftp_client)
    for path, sftp_attr in matching_files:
        if is_dir(sftp_attr):
            console.print(f"[red]{path}: is a directory[/red]")
            return
        sftp_client.remove(str(path))


@handle_io_error(console)
def rmdir(sftp_client: SFTPClient, *args):
    parser = ArgumentParser("rmdir", add_help=False, exit_on_error=False)
    parser.add_argument("directories", nargs="+", type=PurePath, help="Path to list")
    parser.add_argument("--help", action="store_true", help="Show")

    args = parse_args(parser, args)

    matching_files = expand_path_globs(args.directories, sftp_client)
    for path, sftp_attr in matching_files:
        if not is_dir(sftp_attr):
            console.print(f"[red]{path}: Not a directory[/red]")
            return
        sftp_client.rmdir(str(path))


@handle_io_error(console)
def mkdir(sftp_client: SFTPClient, *args):
    parser = ArgumentParser("rmdir", add_help=False, exit_on_error=False)
    parser.add_argument("directories", nargs="+", type=PurePath, help="Path to list")
    parser.add_argument("--help", action="store_true", help="Show")

    args = parse_args(parser, args)

    for path in args.directories:
        sftp_client.mkdir(str(path))


@handle_io_error(console)
def cp(sftp_client: SFTPClient, *args):
    parser = ArgumentParser("cp", add_help=False, exit_on_error=False)
    parser.add_argument("src", type=PurePath, help="Path to list")
    parser.add_argument("dst", type=PurePath, help="Path to list")
    parser.add_argument("--help", action="store_true", help="Show")

    args = parse_args(parser, args)

    src_matching_files = expand_path_globs([args.src], sftp_client)

    if not src_matching_files:
        console.print(f"[red]File {args.src} not found")
        return

    try:
        dst_attr = sftp_client.stat(str(args.dst))
    except IOError:
        dst_attr = None

    if len(src_matching_files) > 1 and (dst_attr is None or not is_dir(dst_attr)):
        console.print(f"[red]{args.dst}: Not a directory")
        return

    with TemporaryDirectory() as tmp_dir:
        tmp_dir_path = Path(tmp_dir)
        for src_file, sftp_attr in src_matching_files:
            dst_name = (
                args.dst / src_file.name if dst_attr and is_dir(dst_attr) else args.dst
            )
            sftp_client.get(str(src_file), str(tmp_dir_path / src_file.name))
            sftp_client.put(str(tmp_dir_path / src_file.name), str(dst_name))


@handle_io_error(console)
def mv(sftp_client: SFTPClient, *args):
    parser = ArgumentParser("cp", add_help=False, exit_on_error=False)
    parser.add_argument("src", nargs="+", type=PurePath, help="Path to list")
    parser.add_argument("dst", type=PurePath, help="Path to list")
    parser.add_argument("--help", action="store_true", help="Show")
    args = parse_args(parser, args)

    src_matching_files = expand_path_globs(args.src, sftp_client)

    try:
        dst_attr = sftp_client.stat(str(args.dst))
    except IOError:
        dst_attr = None

    if len(src_matching_files) > 1 and (dst_attr is None or not is_dir(dst_attr)):
        console.print(f"[red]{args.dst}: Not a directory")
        return

    for src_file, sftp_attr in src_matching_files:
        dst_name = (
            args.dst / src_file.name if dst_attr and is_dir(dst_attr) else args.dst
        )
        sftp_client.posix_rename(str(src_file), str(dst_name))


ALIAS = {
    "ll": ["ls", "-l", "-h"],
    "l": ["ls", "-l", "-h"],
}

COMMANDS = {
    "ls": ls,
    "cd": cd,
    "get": get,
    "put": put,
    "rm": rm,
    "rmdir": rmdir,
    "mkdir": mkdir,
    "cp": cp,
    "mv": mv,
}


def _repl_main(sftp_client: SFTPClient, url: SftpUrl):
    console_interactor = ConsoleInteractor(console, sftp_client, url)
    history_file = configure_readline(console_interactor)
    sftp_client.chdir(url.path or "/")
    while True:
        console_interactor.clear_cache()
        try:
            user_input = console_interactor.get_input()
        except EOFError:
            raise typer.Exit()
        except KeyboardInterrupt:
            print("\n")
            continue

        tokens = shlex.split(user_input.strip())
        if not tokens:
            continue
        if tokens[0] in ALIAS:
            tokens = ALIAS[tokens[0]] + tokens[1:]

        match tokens:
            case ["exit"] | ["quit"]:
                raise typer.Exit()
            case ["pwd"]:
                console.print(f"[bold cyan]{console_interactor.cwd}[/bold cyan]")
            case [command, *args] if command in COMMANDS:
                try:
                    COMMANDS[command](sftp_client, *args)
                except ParserError:
                    pass
            case [command, *_] if command in {"exit", "quit", "help", "pwd"}:
                console.print(f"[red]{command}: too many args[/red]")
            case _:
                console.print(f"Unrecognized command: {user_input}")

        if history_file:
            readline.write_history_file(history_file)

    raise RuntimeError("Unreachable code")


@app.command()
def main(connection_str: str):
    url = TypeAdapter(SftpUrl).validate_python(connection_str)
    password = url.password or getpass.getpass("password: ")
    with SSHClient() as client:
        client.load_system_host_keys()
        client.set_missing_host_key_policy(WarningPolicy())
        client.connect(
            hostname=url.host,
            port=url.port or 22,
            username=url.username,
            password=password,
        )
        print(f"Connected to {url.host}:{url.port or 22} as {url.username}")
        return _repl_main(client.open_sftp(), url)


if __name__ == "__main__":
    sys.exit(app())
