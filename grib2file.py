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
            # TODO: Looks like an unnecessary dependency
            async with aiohttp.ClientSession() as session:
                async with session.get(self._url) as resp:
                    if resp.status != 200:
                        raise ErrorGRIB2FielNotFount(resp.status)
                    
                    data = await resp.read()
                    self._tmp_fp = tempfile.NamedTemporaryFile(mode='wb')
                    await self._loop.run_in_executor(None, self._write_tmp, self._tmp_fp, data)

                    if self._url.endswith('.bz2'):
                        bz2_fp = bz2.open(filename=self._tmp_fp.name, mode='rb')
                        self._fp = bz2_fp

                    else:
                        self._fp = open(self._tmp_fp.name, mode='rb')

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
        # TODO: Figure it out. Are you sure this is better than blocking?
        resutl = await self._loop.run_in_executor(None, self._read, size)
        return resutl
    
    def seek(self, offset: int):
        return self._fp.seek(offset)
    
    def tell(self) -> int:
        return self._fp.tell()
