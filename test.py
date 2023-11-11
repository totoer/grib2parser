
import asyncio

import numpy as np
from PIL import Image

from grib2file import GRIB2File
from grib2 import GRIB2


MAGIC = 200000
URL = 'file://./fixture/cdas1.t00z.ipvgrbanl.grib2'


async def main():
    loop = asyncio.get_running_loop()
    async with GRIB2File(loop, URL) as grib_file:
        grib = GRIB2(grib_file)
        idx = 0

        values = []
        async for m in grib.messages():
            i = Image.new('RGB', (m.s3.ni+1, m.s3.nj+1,))

            for x in range(m.s3.ni):
                for y in range(m.s3.nj):
                    v = m.s7.next()
                    r = int((v / MAGIC) * 255)
                    i.putpixel((x, y,), (r, 0, 0,))

            i.save(f'./fixture/{idx}_test.png')
            idx += 1


if __name__ == '__main__':
    asyncio.run(main())
