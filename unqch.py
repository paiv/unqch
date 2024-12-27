#!/usr/bin/env python
import logging
import sqlite3
import struct
import sys
import zlib
from pathlib import Path


logger = logging.getLogger(Path(__file__).stem)


def unpack_contents_table(data):
    off = 0
    while off < len(data):
        lvl, = struct.unpack('>I', data[off:off+4])
        off += 4

        fsz, = struct.unpack('>I', data[off:off+4])
        off += 4
        chk = data[off:off+fsz]
        off += fsz
        name = chk.decode('utf-16be')

        ttn, = struct.unpack('>I', data[off:off+4])
        off += 4
        chk = data[off:off+ttn]
        off += ttn
        title = chk.decode('utf-16be')

        yield (lvl, name, title)


def write_contents_table(con, target):
    cur = con.cursor()
    res = cur.execute('select Data from ContentsTable;')
    with target.open('w') as so:
        for data, in res:
            for n, fn, title in unpack_contents_table(data):
                ws = '  ' * n
                print(f'{ws}- [{title}]({fn})', file=so)


def write_files(con, folder_id, target):
    cur = con.cursor()
    res = cur.execute('''\
select fn.FileId, fn.Name, fn.Title, dt.Data
from FileDataTable dt
join FileNameTable fn on dt.Id = fn.FileId
where fn.FolderId = ?
and fn.Name not null
and dt.Data not null
;
        ''', (folder_id,))
    for fid, name, title, data in res:
        sz = f'{len(data)} bytes' if data else 'empty'
        logger.debug(f'{fid=} ({sz}) {name!r} {title!r}')
        fn = target / Path(name)
        if fn.exists():
            raise Exception(f'file exists: {fn}')
        fn.parent.mkdir(parents=True, exist_ok=True)
        text = zlib.decompress(data[4:])
        fn.write_bytes(text)


def enumerate_folders(con):
    cur = con.cursor()
    res = cur.execute('select Id, Name from FolderTable;')
    yield from res
 

def main(args):
    target = Path(args.output) if args.output else Path(Path(args.file).stem)
    if target.exists():
        print(f'! file exists: {target!s}', file=sys.stderr)
        exit(1)

    uri = f'file:{args.file!s}?mode=ro'
    logger.debug(f'open db {uri!r}')
    con = sqlite3.connect(uri, uri=True)

    logger.debug(f'create target {target!s}')
    target.mkdir(parents=True, exist_ok=True)

    write_contents_table(con, target / 'toc.md')

    for fid, name in enumerate_folders(con):
        write_files(con, fid, target / name)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output', help='output directory')
    parser.add_argument('-v', '--verbose', action='store_true', help='verbose output')
    parser.add_argument('file', help='QCH file')
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(level=level)

    main(args)
