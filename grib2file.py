from __future__ import annotations

import io
import bz2
import asyncio
import aiohttp
import tempfile


class ErrorUnsuportedURL(Exception): pass


class ErrorGRIB2FielNotFount(Exception):

    def __init__(self, code: int) -> None:
        super().__init__(f'Response code: {code}')


class GRIB2File:

    def __init__(self, loop: asyncio.AbstractEventLoop, url: str):
        self._url = url
        self._loop = loop

    def _write_tmp(self, fp: io.BufferedWriter, data: bytes):
        fp.write(data)
        fp.flush()

    async def open(self) -> GRIB2File:
        if self._url.startswith('http'):
            async with aiohttp.ClientSession() as session:
                async with session.get(self._url) as resp:
                    if resp.status != 200:
                        raise ErrorGRIB2FielNotFount(resp.status)
                    data = await resp.read()
                    tmp_fp = tempfile.NamedTemporaryFile(mode='wb')
                    await self._loop.run_in_executor(None, self._write_tmp, tmp_fp, data)
                    bz2_fp = bz2.open(filename=tmp_fp.name, mode='rb')
                    self._tmp_fp = tmp_fp
                    self._fp = bz2_fp

        elif self._url.startswith('file'):
            path = self._url[len('file://'):]
            self._tmp_fp = None
            self._fp = open(path, 'rb')

        else:
            raise ErrorUnsuportedURL()

    async def __aenter__(self):
        await self.open()
        return self
    
    async def __aexit__(self, *args, **kwargs):
        self.close()

    def close(self):
        self._fp.close()
        if self._tmp_fp:
            self._tmp_fp.close()

    def _read(self, size: int):
        return self._fp.read(size)

    async def read(self, size: int):
        resutl = await self._loop.run_in_executor(None, self._read, size)
        return resutl
    
    def seek(self, offset: int):
        return self._fp.seek(offset)
    
    def tell(self) -> int:
        return self._fp.tell()
