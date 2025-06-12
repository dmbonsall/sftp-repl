import shlex
import sys
from argparse import ArgumentParser, ArgumentError
from pathlib import Path
from typing import Sequence

import typer
from paramiko.client import WarningPolicy
from paramiko.sftp_client import SFTPClient
from pydantic import TypeAdapter
from paramiko import SSHClient
from rich.columns import Columns
from rich.console import Console
from rich.progress import Progress

from sftp_repl.completions import is_dir, ConsoleInteractor, configure_readline
from sftp_repl.utils import format_name, long_listing, SftpUrl

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


def ls(sftp_client: SFTPClient, *args):
    """List files in the specified directory."""
    parser = ArgumentParser("ls", add_help=False, exit_on_error=False)
    parser.add_argument("path", nargs="?", default=".", type=Path, help="Path to list")
    parser.add_argument("--help", action="store_true", help="Show")
    parser.add_argument(
        "-l", action="store_true", dest="long", help="Long listing format"
    )
    parser.add_argument(
        "-h", action="store_true", dest="human", help="Human readable sizes"
    )
    args = parse_args(parser, args)

    try:
        files = [sftp_client.stat(str(args.path))]
        files[0].filename = args.path.name
        if is_dir(files[0]):
            files = sftp_client.listdir_attr(str(args.path))
        if args.long:
            for file in sorted(
                files,
                key=lambda f: f.filename.lower(),
            ):
                console.print(
                    long_listing(file, human_readable=args.human), highlight=False
                )
        else:
            formatted_files = [
                format_name(f) for f in sorted(files, key=lambda f: f.filename.lower())
            ]
            console.print(Columns(formatted_files))
    except IOError as ex:
        console.print(f"[red]{ex}[/red]")


def cd(sftp_client: SFTPClient, *args):
    parser = ArgumentParser("cd", add_help=False, exit_on_error=False)
    parser.add_argument("path", help="Path to change to")
    parser.add_argument("--help", action="store_true", help="Show")
    args = parse_args(args)

    try:
        sftp_client.chdir(args.path)
    except IOError as ex:
        console.print(f"[red]{ex}[/red] ")


def get(sftp_client: SFTPClient, *args):
    parser = ArgumentParser("get", add_help=False, exit_on_error=False)
    parser.add_argument("src", type=Path, help="source path")
    parser.add_argument(
        "dst", type=Path, nargs="?", default=None, help="destination path"
    )
    parser.add_argument("--help", action="store_true", help="Show")
    args = parse_args(args)

    if args.dst is None:
        args.dst = args.src.name

    with Progress() as progress:
        full_path = (Path(sftp_client.getcwd()) / args.src).absolute()
        task = progress.add_task(
            f"[cyan]Fetching {full_path} to {args.dst}[/cyan]",
        )

        update_called = False

        def _update(current, total):
            # for empty files
            nonlocal update_called
            update_called = True
            if total == 0:
                total, current = 1, 1
            progress.update(task, completed=current, total=total, refresh=True)

        try:
            sftp_client.get(str(args.src), str(args.dst), callback=_update)
        except IOError as ex:
            console.print(f"[red]{ex}[/red] ")
        if not update_called:
            progress.update(task, completed=1, total=1, refresh=True)


def put(sftp_client: SFTPClient, *args):
    parser = ArgumentParser("put", add_help=False, exit_on_error=False)
    parser.add_argument("src", type=Path, help="source path")
    parser.add_argument(
        "dst", type=Path, nargs="?", default=None, help="destination path"
    )
    parser.add_argument("--help", action="store_true", help="Show")
    args = parse_args(args)

    if args.dst is None:
        args.dst = args.src.name

    with Progress() as progress:
        full_path = (Path(sftp_client.getcwd()) / args.dst).absolute()
        task = progress.add_task(
            f"[cyan]Uploading {args.src} to {full_path}[/cyan]",
        )

        update_called = False

        def _update(current, total):
            # for empty files
            nonlocal update_called
            update_called = True
            if total == 0:
                total, current = 1, 1
            progress.update(task, completed=current, total=total, refresh=True)

        try:
            sftp_client.put(str(args.src), str(args.dst), callback=_update)
        except IOError as ex:
            console.print(f"[red]{ex}[/red] ")
        if not update_called:
            progress.update(task, completed=1, total=1, refresh=True)


ALIAS = {
    "ll": ["ls", "-l", "-h"],
    "l": ["ls", "-l", "-h"],
}

COMMANDS = {
    "ls": ls,
    "cd": cd,
    "get": get,
    "put": put,
}


def _repl_main(sftp_client: SFTPClient, url: SftpUrl):
    console_interactor = ConsoleInteractor(console, sftp_client, url)
    configure_readline(console_interactor)
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

        tokens = shlex.split(user_input)
        if tokens[0] in ALIAS:
            tokens = ALIAS[tokens[0]] + tokens[1:]

        match tokens:
            case ["exit"] | ["quit"]:
                raise typer.Exit()
            case ["pwd"]:
                console.print(console_interactor.cwd)
            case [command, *args] if command in COMMANDS:
                try:
                    COMMANDS[command](sftp_client, *args)
                except ParserError:
                    pass
            case [command, *_] if command in {"exit", "quit", "help", "pwd"}:
                console.print(f"[red]pwd: too many args[/red]")
            case _:
                console.print(f"Unrecognized command: {user_input}")

    raise RuntimeError("Unreachable code")


@app.command()
def main(connection_str: str):
    url = TypeAdapter(SftpUrl).validate_python(connection_str)
    with SSHClient() as client:
        client.load_system_host_keys()
        client.set_missing_host_key_policy(WarningPolicy())
        client.connect(
            hostname=url.host,
            port=url.port or 22,
            username=url.username,
            password=url.password,
        )
        print(f"Connected to {url.host}:{url.port or 22} as {url.username}")
        return _repl_main(client.open_sftp(), url)


if __name__ == "__main__":
    sys.exit(app())
