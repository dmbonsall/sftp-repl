import shlex
import stat
import sys
from argparse import ArgumentParser, ArgumentError
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Annotated
import readline
import time

import typer
from paramiko.client import WarningPolicy
from paramiko.sftp_attr import SFTPAttributes
from paramiko.sftp_client import SFTPClient
from pydantic import UrlConstraints, TypeAdapter
from pydantic.networks import AnyUrl
from paramiko import SSHClient
from rich import print
from rich.columns import Columns
from rich.console import Console
from rich.progress import Progress

console = Console()
SftpUrl = Annotated[AnyUrl, UrlConstraints(allowed_schemes=["sftp"])]


def is_dir(sftp_attr: SFTPAttributes) -> bool:
    return stat.S_IFMT(sftp_attr.st_mode) == stat.S_IFDIR


def format_completion(sftp_attr: SFTPAttributes):
    if is_dir(sftp_attr):
        return f"{sftp_attr.filename}/"
    return f"{sftp_attr.filename}"


@dataclass
class Token:
    text: str
    start: int
    end: int


def tokenize(line: str) -> list[Token]:
    str_tokens = shlex.split(line)
    tokens = []
    start = 0
    for token in str_tokens:
        start = line[start:].find(token)
        end = start + len(token)
        tokens.append(Token(text=token, start=start, end=end))
    return tokens


def locate_full_token(tokens: list[Token], begin: int, end: int):
    for token in tokens:
        if begin >= token.start and begin <= token.end:
            assert end <= token.end, "End of token is expected to be within the token"
            return token
    assert False, "unreachable code"


@dataclass
class Completer:
    sftp_client: SFTPClient

    def complete(self, text, state):
        possible_completions = sorted(self.file_completions_for_text(text))
        if state >= len(possible_completions):
            return None
        return possible_completions[state]

    def file_completions_for_text(self, text):
        line = readline.get_line_buffer()
        tokens = tokenize(line)
        token = locate_full_token(tokens, readline.get_begidx(), readline.get_endidx())

        path = PurePosixPath(token.text)
        if text == "":
            parent, name = str(path), ""
        else:
            parent, name = str(path.parent), path.name

        try:
            sftp_attr = self.sftp_client.stat(parent)
        except IOError:
            return []

        if not is_dir(sftp_attr):
            return []

        files = self.sftp_client.listdir_attr(parent)
        return [format_completion(f) for f in files if f.filename.startswith(name)]


def format_name(sftp_attr):
    kind = stat.S_IFMT(sftp_attr.st_mode)
    filename = getattr(sftp_attr, "filename", "?")
    if kind == stat.S_IFDIR:
        return f"[bold cyan]{filename}[/bold cyan]"
    if kind == stat.S_IFLNK:
        return f"[purple]{filename}[/purple]"
    return filename


def human_readable_size(size: int) -> str:
    """Convert a size in bytes to a human-readable format."""
    if size < 1024:
        ssize, label = size, "B"
    elif size < 1024**2:
        ssize, label = size / 1024, "K"
    elif size < 1024**3:
        ssize, label = size / (1024**2), "M"
    elif size < 1024**4:
        ssize, label = size / (1024**3), "G"
    else:
        ssize, label = size / (1024**4), "T"

    if ssize > 10.0 or size < 1024:
        return f"{ssize:.0f}{label}"
    return f"{ssize:.1f}{label}"


def long_listing(sftp_attr, human_readable=False):
    """create a unix-style long description of the file (like ls -l).

    Copied from paramiko and updated
    """

    if sftp_attr.st_mode is not None:
        kind = stat.S_IFMT(sftp_attr.st_mode)
        if kind == stat.S_IFIFO:
            ks, file_colo = "p", "default"
        elif kind == stat.S_IFCHR:
            ks, file_colo = "c", "default"
        elif kind == stat.S_IFDIR:
            ks, file_colo = "d", "bold cyan"
        elif kind == stat.S_IFBLK:
            ks, file_colo = "b", "default"
        elif kind == stat.S_IFREG:
            ks, file_colo = "-", "default"
        elif kind == stat.S_IFLNK:
            ks, file_colo = "l", "purple"
        elif kind == stat.S_IFSOCK:
            ks, file_colo = "s", "default"
        else:
            ks, file_colo = "?", "default"
        ks += sftp_attr._rwx(
            (sftp_attr.st_mode & 0o700) >> 6, sftp_attr.st_mode & stat.S_ISUID
        )
        ks += sftp_attr._rwx(
            (sftp_attr.st_mode & 0o70) >> 3, sftp_attr.st_mode & stat.S_ISGID
        )
        ks += sftp_attr._rwx(
            sftp_attr.st_mode & 7, sftp_attr.st_mode & stat.S_ISVTX, True
        )
    else:
        ks, file_colo = "?---------", "default"
    # compute display date
    if (sftp_attr.st_mtime is None) or (sftp_attr.st_mtime == 0xFFFFFFFF):
        # shouldn't really happen
        datestr = "(unknown date)"
    else:
        time_tuple = time.localtime(sftp_attr.st_mtime)
        if abs(time.time() - sftp_attr.st_mtime) > 15_552_000:
            # (15,552,000s = 6 months)
            datestr = time.strftime("%d %b %Y", time_tuple)
        else:
            datestr = time.strftime("%d %b %H:%M", time_tuple)
    filename = format_name(sftp_attr)

    # not all servers support uid/gid
    uid = sftp_attr.st_uid
    gid = sftp_attr.st_gid
    size = sftp_attr.st_size
    if uid is None:
        uid = 0
    if gid is None:
        gid = 0
    if size is None:
        size = 0
    if human_readable:
        size_str = human_readable_size(size)

        return f"{ks}   1 {uid:<8d} {gid:<8d} {size_str:>8s} {datestr:12s} {filename}"
    return "%s   1 %-8d %-8d %8d %-12s %s" % (
        ks,
        uid,
        gid,
        size,
        datestr,
        filename,
    )


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
    try:
        args = parser.parse_args(args)
    except ArgumentError as ex:
        console.print(parser.format_usage(), end="")
        console.print(f"[red]{ex.message}[/red]")
        return

    if args.help:
        console.print(parser.format_help())
        return

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
        console.print(f"[red]{ex}[/red] ")


def cd(sftp_client: SFTPClient, *args):
    parser = ArgumentParser("cd", add_help=False, exit_on_error=False)
    parser.add_argument("path", help="Path to change to")
    try:
        args = parser.parse_args(args)
    except ArgumentError as ex:
        console.print(parser.format_usage(), end="")
        console.print(f"[red]{ex.message}[/red]")
        return

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
    try:
        args = parser.parse_args(args)
    except ArgumentError as ex:
        console.print(parser.format_usage(), end="")
        console.print(f"[red]{ex.message}[/red]")
        return

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
    try:
        args = parser.parse_args(args)
    except ArgumentError as ex:
        console.print(parser.format_usage(), end="")
        console.print(f"[red]{ex.message}[/red]")
        return

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


def configure_readline(sftp_client: SFTPClient):
    readline.set_completer(Completer(sftp_client).complete)

    # from python cmd library
    if readline.backend == "editline":
        command_string = "bind ^I rl_complete"
    else:
        command_string = f"tab: complete"
    readline.parse_and_bind(command_string)


alias = {
    "ll": ["ls", "-l", "-h"],
    "l": ["ls", "-l", "-h"],
}

commands = {
    "ls": ls,
    "cd": cd,
    "get": get,
    "put": put,
}


def _repl_main(sftp_client: SFTPClient, url: SftpUrl) -> int:
    configure_readline(sftp_client)
    sftp_client.chdir(url.path or "/")
    while True:
        cwd = sftp_client.getcwd()
        ps1 = f"[green]{url.username}@{url.host}[/green]:[blue]{cwd}[/blue] > "
        try:
            with console.capture() as capture:
                console.print(ps1, end="")
            user_input = input(capture.get())
        except EOFError:
            return 0
        except KeyboardInterrupt:
            print("\n")
            continue

        tokens = shlex.split(user_input)
        if tokens[0] in alias:
            tokens = alias[tokens[0]] + tokens[1:]

        match tokens:
            case ["exit"] | ["quit"]:
                return 0
            case ["pwd"]:
                console.print(cwd)
            case [command, *args] if command in commands:
                commands[command](sftp_client, *args)
            case [command, *_] if command in {"exit", "quit", "help", "pwd"}:
                console.print(f"[red]pwd: too many args[/red]")
            case _:
                console.print(f"Unrecognized command: {user_input}")

    raise RuntimeError("Unreachable code")


if __name__ == "__main__":
    sys.exit(typer.run(main))
