
import os
import sys
import asyncio
import configparser
import logging
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta


from grib2file import GRIB2File, ErrorGRIB2FielNotFount
from grib2 import GRIB2
from wgf4 import WGF4, WGF4Headers
from picture import dump_to_image


parser = argparse.ArgumentParser(prog='WGF4')
parser.add_argument('-c', '--config',
                    default='./fixture/config.ini', help='path to config file')
parser.add_argument('-d', '--date', help='date of source dataset')
parser.add_argument('-r', '--range', help='count datasets by date')
parser.add_argument('-t', '--target', help='result type: WGF4 or picture')
args = parser.parse_args()

config = configparser.ConfigParser()
config.read(args.config)


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


async def _dump_to_wgf4(loop: asyncio.AbstractEventLoop, grib: GRIB2, d: datetime):
    async with WGF4(loop=loop, workdir=config.get('base', 'workdir'), d=d) as wgf4:                
        wgf4_headers = None
        
        async for m in grib.messages():
            if not wgf4_headers:
                # TODO: Can la1, la2 etc be different among messages?
                # TODO: Calc the multiplier correct
                wgf4_headers = WGF4Headers(
                            latitude1=m.s3.la1, latitude2=m.s3.la2,
                            longtituge1=m.s3.lo1, longtituge2=m.s3.lo2,
                            latitude=m.s3.ni, longtituge=m.s3.nj,
                            multiplier=1)

            for v in m.s7.cunks():
                await wgf4.write(v)
        
        await wgf4.set_headers(wgf4_headers)

        wgf4.save()


async def _dump_to_picture(idx: int, grib: GRIB2, d: datetime):
    async for message in grib.messages():
        dump_to_image(idx=idx, message=message, workdir=config.get('base', 'workdir'), d=d)


async def process(idx: int, d: datetime, loop: asyncio.AbstractEventLoop, url: str):
    try:
        url = url % { 'idx': idx, }

        async with GRIB2File(loop, url) as grib_file:
            grib = GRIB2(grib_file)

            if args.target == 'WGF4':
                dd = d + timedelta(hours=idx)
                await _dump_to_wgf4(loop=loop, grib=grib, d=dd)

            elif args.target == 'picture':
                await _dump_to_picture(idx=idx, grib=grib, d=d)
            
        logging.info('Job %d has ben done', idx)

    except ErrorGRIB2FielNotFount as ex:
        logging.error('Job %d has has been failed %s, link %s', idx, ex, url)


async def main():
    if not os.path.isdir(config.get('base', 'workdir')):
        os.mkdir(config.get('base', 'workdir'))

    try:
        d = datetime.strptime(args.date, '%Y-%m-%d:%H')
    except:
        d = datetime.strptime(args.date, '%Y-%m-%d')

    url_template = config.get('base', 'url_template')
    r = int(args.range)

    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=config.getint('base', 'workers')) as pool:
        loop.set_default_executor(pool)

        c = []
        for idx in range(0, r):
            url = datetime.strftime(d, url_template)
            c.append(process(idx=idx, d=d, loop=loop, url=url))

        await asyncio.gather(*c)
        logging.info('done')


if __name__ == '__main__':
    asyncio.run(main())
