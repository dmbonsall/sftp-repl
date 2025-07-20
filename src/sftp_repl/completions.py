import os
import shlex
import stat
import sys
from dataclasses import dataclass, field
from pathlib import PurePosixPath, Path
from typing import Sequence

import readline

if readline.backend != "readline":
    try:
        import gnureadline as readline
    except ImportError:

        pass

from paramiko import SFTPAttributes, SFTPClient
from rich.console import Console
from rich.columns import Columns

from sftp_repl.utils import SftpUrl


def is_dir(sftp_attr: SFTPAttributes) -> bool:
    return stat.S_IFMT(sftp_attr.st_mode) == stat.S_IFDIR


def format_completion(sftp_attr: SFTPAttributes):
    if is_dir(sftp_attr):
        return f"[bold cyan]{sftp_attr.filename}/[/bold cyan]"
    return f"{sftp_attr.filename}"


def format_completion_no_color(sftp_attr: SFTPAttributes):
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
        start = line[start:].find(token) + start
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
class DirectoryCache:
    sftp_client: SFTPClient
    _files_by_directory: dict[PurePosixPath, list[SFTPAttributes]] = field(
        default_factory=dict
    )
    _files_by_fullname: dict[PurePosixPath, SFTPAttributes] = field(
        default_factory=dict
    )
    _cwd: str | None = None

    @property
    def cwd(self) -> str:
        if self._cwd is None:
            self._cwd = self.sftp_client.getcwd()
        return self._cwd

    def listdir(self, path: PurePosixPath):
        if path not in self._files_by_directory:
            try:
                self._files_by_directory[path] = self.sftp_client.listdir_attr(
                    str(path)
                )
            except IOError:
                self._files_by_directory[path] = []
        return self._files_by_directory[path]

    def is_directory(self, path: PurePosixPath) -> bool:
        if path in self._files_by_directory:
            return True
        if path.parent in self._files_by_directory:
            for file in self._files_by_directory[path.parent]:
                if file.filename == path.name:
                    return is_dir(file)

        if path not in self._files_by_fullname:
            try:
                sftp_attr = self.sftp_client.stat(str(path))
            except IOError:
                return False
            self._files_by_fullname[path] = sftp_attr
        return is_dir(self._files_by_fullname[path])


@dataclass
class ConsoleInteractor:
    console: Console
    sftp_client: SFTPClient
    url: SftpUrl
    _directory_cache: DirectoryCache = None

    def __post_init__(self):
        self._directory_cache = DirectoryCache(self.sftp_client)

    @property
    def directory_cache(self):
        return self._directory_cache

    def clear_cache(self):
        self._directory_cache = DirectoryCache(self.sftp_client)

    @property
    def cwd(self) -> str:
        return self._directory_cache.cwd

    @property
    def ps1(self) -> str:
        return f"[green]{self.url.username}@{self.url.host}[/green]:[blue]{self.cwd}[/blue] > "

    def get_input(self):
        with self.console.capture() as capture:
            self.console.print(self.ps1, end="")
        return input(capture.get())

    def completion_display_matches_hook(
        self, substitution: str, matches: Sequence[str], longest_match_length: int
    ):
        formatted_matches = [
            format_completion(self.match_attr_cache[m]) for m in matches
        ]
        self.console.print()
        self.console.print(Columns(formatted_matches, padding=(0, 4)), end="", sep="")
        self.console.print(self.ps1, readline.get_line_buffer(), end="", sep="")
        readline.redisplay()

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
            parent, name = path, ""
        else:
            parent, name = path.parent, path.name

        if not self.directory_cache.is_directory(parent):
            return []

        files = self.directory_cache.listdir(parent)
        self.match_attr_cache = {
            format_completion_no_color(f): f
            for f in files
            if f.filename.startswith(name)
        }
        return list(self.match_attr_cache.keys())


def configure_readline(console_interactor: ConsoleInteractor) -> Path | None:
    readline.set_completer(console_interactor.complete)
    if readline.backend == "readline" or sys.platform != "darwin":
        # MacOS default implementation of editline doesn't work with the hook, this
        # article explains it https://pewpewthespells.com/blog/osx_readline.html
        readline.set_completion_display_matches_hook(
            console_interactor.completion_display_matches_hook
        )

    # from python cmd library
    if readline.backend == "editline":
        command_string = "bind ^I rl_complete"
    else:
        command_string = f"tab: complete"
    readline.parse_and_bind(command_string)

    if home := os.getenv("HOME"):
        history_file = Path(home) / ".sftp_history"
        if history_file.is_file():
            readline.read_history_file(history_file)
        else:
            history_file.open("w").close()
        readline.set_auto_history(True)
        readline.set_history_length(1000)
        return history_file
