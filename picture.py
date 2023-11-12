
import os
from datetime import datetime

from PIL import Image

from grib2 import GRIB2Message


MAGIC = 200000


def dump_to_image(idx: int, message: GRIB2Message, workdir: str, d: datetime):
    image = Image.new('RGB', (message.s3.ni+1, message.s3.nj+1,))

    for v in message.s7.cunks():
        for x in range(message.s3.ni):
            for y in range(message.s3.nj):
                v = message.s7.next()
                r = int((v / MAGIC) * 255)
                image.putpixel((x, y,), (r, 0, 0,))

        filepath = os.path.join(workdir, f'{datetime.strftime(d, "%Y-%m-%d")}_{idx}.png')
        image.save(filepath)
