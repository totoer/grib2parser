
import os
import sys
import asyncio
import configparser
import logging
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta


from grib2file import GRIB2File, ErrorGRIB2FielNotFount
from wgf4 import WGF4, WGF4Headers
from grib2 import GRIB2


parser = argparse.ArgumentParser(prog='WGF4')
parser.add_argument('-c', '--config', default='./fixture/config.ini')
parser.add_argument('-d', '--date')
parser.add_argument('-i', '--hour')
args = parser.parse_args()

config = configparser.ConfigParser()
config.read(args.config)


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


async def process(i: int, d: datetime, loop: asyncio.AbstractEventLoop, url: str):
    try:
        # TODO: Move it to WGF4
        current_time = d + timedelta(hours=i)
        datetime_part = datetime.strftime(current_time, '%d.%m.%Y_%H:00')
        foldername = '%s_%d' % (datetime_part, int(current_time.timestamp()))
        folderpath = os.path.join(config.get('base', 'workdir'), foldername)
        if not os.path.isdir(folderpath):
            os.mkdir(folderpath)
        filepath = os.path.join(folderpath, 'PRATE.wgf4')

        async with GRIB2File(loop, url) as grib_file:
            grib = GRIB2(grib_file)

            async with WGF4(loop=loop, path=filepath) as wgf4:                
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
        logging.info('Job %d has ben done', i)

    except ErrorGRIB2FielNotFount as ex:
        logging.error('Job %d has has been failed %s, link %s', i, ex, url)


async def main():
    if not os.path.isdir(config.get('base', 'workdir')):
        os.mkdir(config.get('base', 'workdir'))

    d = datetime.strptime(f'{args.date} {args.hour}', '%Y-%m-%d %H')

    date = args.date.replace('-', '')
    r_date = f'{date}{args.hour}'

    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=config.getint('base', 'workers')) as pool:
        loop.set_default_executor(pool)

        c = []
        for i in range(0, 48):
            url = config.get('base', 'url_template') % (args.hour, r_date, i,)
            c.append(process(i=i, d=d, loop=loop, url=url))

        await asyncio.gather(*c)
        logging.info('done')


if __name__ == '__main__':
    asyncio.run(main())
