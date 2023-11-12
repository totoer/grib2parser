
import os
import tempfile
import shutil
import struct
import asyncio
from datetime import datetime
from io import BufferedWriter
from dataclasses import dataclass


@dataclass
class WGF4Headers:

    # координата latitude1 (нижняя широта)
    latitude1: int

    # координата latitude2 (верхняя широта)
    latitude2: int

    # координата longtituge1 (левая долгота)
    longtituge1: int

    # координата longtituge2 (права долгота)
    longtituge2: int

    # шаг сетки latitude
    latitude: int

    # шаг сетки longtituge
    longtituge: int

    # множитель на который умножаются все значения хидера, чтобы избавиться от дробных частей
    multiplier: int

    def dump(self, fp: BufferedWriter):
        r_latitude1 = struct.pack('>I', self.latitude1)
        fp.write(r_latitude1)

        r_latitude2 = struct.pack('>I', self.latitude2)
        fp.write(r_latitude2)

        r_longtituge1 = struct.pack('>I', self.longtituge1)
        fp.write(r_longtituge1)

        r_longtituge2 = struct.pack('>I', self.longtituge2)
        fp.write(r_longtituge2)

        r_latitude = struct.pack('>I', self.latitude)
        fp.write(r_latitude)

        r_longtituge = struct.pack('>I', self.longtituge)
        fp.write(r_longtituge)

        r_multiplier = struct.pack('>I', self.multiplier)
        fp.write(r_multiplier)


class WGF4:

    def __init__(self, loop: asyncio.AbstractEventLoop, workdir: str, d: datetime):
        self._loop = loop
        
        datetime_part = datetime.strftime(d, '%d.%m.%Y_%H:00')
        foldername = '%s_%d' % (datetime_part, int(d.timestamp()))
        folderpath = os.path.join(workdir, foldername)
        if not os.path.isdir(folderpath):
            os.mkdir(folderpath)
        
        self._path = os.path.join(folderpath, 'PRATE.wgf4')
        self._fp = tempfile.NamedTemporaryFile()
        headers_size = 7 * 4 + 4
        self._fp.write(bytes(headers_size))

    def _set_headersers(self, headers: WGF4Headers):
        self._fp.seek(0)
        headers.dump(self._fp)
        self._fp.write(struct.pack('>f', -100500))

    async def set_headers(self, headers: WGF4Headers):
        await self._loop.run_in_executor(None, self._set_headersers, headers)

    async def __aenter__(self):
        return self
    
    async def __aexit__(self, *args, **kwargs):
        self.close()

    def close(self):
        self._fp.close()

    def _write(self, v: float):
        data = struct.pack('>f', v)
        self._fp.write(data)

    async def write(self, v: float):
        await self._loop.run_in_executor(None, self._write, v)

    def save(self):
        self._fp.flush()
        shutil.copy(self._fp.name, self._path)
