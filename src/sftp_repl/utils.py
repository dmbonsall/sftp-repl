import functools
import stat
import time
from typing import Annotated

from paramiko.sftp_attr import SFTPAttributes
from pydantic import AnyUrl, UrlConstraints
from rich.console import Console

SftpUrl = Annotated[AnyUrl, UrlConstraints(allowed_schemes=["sftp"])]


def format_name(name: str, sftp_attr: SFTPAttributes) -> str:
    kind = stat.S_IFMT(sftp_attr.st_mode)
    # filename = getattr(sftp_attr, "filename", "?")
    filename = name
    if kind == stat.S_IFDIR:
        return f"[bold cyan]{filename}/[/bold cyan]"
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


def long_listing(name: str, sftp_attr, human_readable=False) -> str:
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
    filename = format_name(name, sftp_attr)

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


def handle_io_error(console: Console):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*a, **k):
            try:
                return func(*a, **k)
            except IOError as ex:
                console.print(f"[red]{ex}[/red]")

        return wrapper

    return decorator
