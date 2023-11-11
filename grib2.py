from __future__ import annotations

import struct
from grib2file import GRIB2File


class CType:

    @property
    def size(self):
        raise NotImplementedError()

    def __init__(self, format: str, size: int):
        self._format = format
        self._size = size

    async def load(self, fp: GRIB2File):
        data = await fp.read(self._size)
        return struct.unpack(self._format, data)[0]
    

class UInt8(CType):

    @property
    def size(self):
        return 1

    def __init__(self):
        super().__init__('>B', self.size)


class UInt16(CType):
    
    @property
    def size(self):
        return 2

    def __init__(self):
        super().__init__('>H', self.size)


class Int16(CType):
    
    @property
    def size(self):
        return 2

    def __init__(self):
        super().__init__('>h', self.size)


class Template(UInt16):

    @property
    def description(self):
        return self._description

    def __init__(self, description):
        self._description = description
        super().__init__()


class UInt32(CType):

    @property
    def size(self):
        return 4

    def __init__(self):
        super().__init__('>I', self.size)


class UInt64(CType):

    @property
    def size(self):
        return 8

    def __init__(self):
        super().__init__('>Q', self.size)


class Float32(CType):

    @property
    def size(self):
        return 4

    def __init__(self):
        super().__init__('>f', self.size)


class ErrorNotSupportedTemplate(Exception):

    def __init__(self, value, s):
        super().__init__(f'Wrong template number {value}. {s}')


class Section:

    @property
    def fields(self) -> tuple[tuple[str, CType]]:
        raise NotImplementedError()

    @property
    def values(self):
        return self._values

    @property
    def size(self):
        return self._size
    
    def __init__(self, fp: GRIB2File, section_len: int):
        self._size = 0
        self._values = {}
        self._fp = fp
        self._section_len = section_len

    async def load(self):
        for name, obj in self.fields:
            value = await obj.load(self._fp)
            self._values[name] = value
            self._size += obj.size

            if isinstance(obj, Template):
                if value not in obj.description:
                    raise ErrorNotSupportedTemplate(value, self)
                
                desc = obj.description[value]

                for name, obj in desc:
                    value = await obj.load(self._fp)
                    self._values[name] = value
                    self._size += obj.size

        if self._section_len > 0:
            reserved_size = self._section_len - self.size
            if reserved_size > 0:
                self._fp.seek(self._fp.tell() + reserved_size)


class Section0(Section):

    @property
    def fields(self):
        return (
        # ('grib', UInt32(),),
            ('skip', UInt16(),),
            ('discipline', UInt8(),),
            ('edition_number', UInt8(),),
            ('total_length', UInt64(),),
        )

    @property
    def discipline(self) -> int:
        return self.values['discipline']

    def __init__(self, fp: GRIB2File):
        super().__init__(fp, -1)


class Section1(Section):

    @property
    def fields(self):
        return (
            # 6-7 Identification of originating/generating center (See Table 0) (See note 4)
            ('identification_center', UInt16(),),
            # 8-9 Identification of originating/generating subcenter (See Table C)
            ('identification_subcenter', UInt16(),),
            # 10 GRIB master tables version number (currently 2) (See Table 1.0) (See note 1)
            ('master_tables_version', UInt8(),),
            # 11 Version number of GRIB local tables used to augment Master Tables (see Table 1.1)
            ('version_local_tables', UInt8(),),
            # 12 Significance of reference time (See Table 1.2)
            ('reference_time', UInt8(),),
            # 13-14 Year (4 digits)
            ('year', UInt16(),),
            # 15 Month
            ('month', UInt8(),),
            # 16 Day
            ('day', UInt8(),),
            # 17 Hour
            ('hour', UInt8(),),
            # 18 Minute
            ('minute', UInt8(),),
            # 19 Second
            ('second', UInt8(),),
            # 20 Production Status of Processed data in the GRIB message (See Table 1.3)
            ('production_status', UInt8(),),
            # 21 Type of processed data in this GRIB message (See Table 1.4)
            ('type_of_data', UInt8(),),
            # 22-N Reserved
        )


class Section3(Section):

    @property
    def grid_templates(self):
        return {
            0: (
                # 15	Shape of the Earth (See Code Table 3.2)
                ('shape_of_earth', UInt8(),),
                # 16	Scale Factor of radius of spherical Earth
                ('scale_factor_of_radius', UInt8(),),
                # 17-20	Scale value of radius of spherical Earth
                ('scale_value_of_radius', UInt32(),),
                # 21	Scale factor of major axis of oblate spheroid Earth
                ('scale_factor_of_major_axis', UInt8(),),
                # 22-25	Scaled value of major axis of oblate spheroid Earth
                ('scaled_factor_of_major_axis', UInt32(),),
                # 26	Scale factor of minor axis of oblate spheroid Earth
                ('scale_factor_of_minor_axis', UInt8(),),
                # 27-30	Scaled value of minor axis of oblate spheroid Earth
                ('scaled_value_of_minor_axis', UInt32(),),
                # 31-34	Ni — number of points along a parallel
                ('ni', UInt32(),),     
                # 35-38	Nj — number of points along a meridian
                ('nj', UInt32(),),
                # 39-42	Basic angle of the initial production domain (see Note 1)
                ('basic_angle', UInt32(),),
                # 43-46	Subdivisions of basic angle used to define extreme longitudes and latitudes, and direction increments (see Note 1)
                ('subdivisions_of_basic_angle', UInt32(),),
                # 47-50	La1 — latitude of first grid point (see Note 1)
                ('la1', UInt32(),),
                # 51-54	Lo1 — longitude of first grid point (see Note 1)
                ('lo1', UInt32(),),
                # 55	Resolution and component flags (see Flag Table 3.3)
                ('resolution', UInt8(),),
                # 56-59	La2 — latitude of last grid point (see Note 1)
                ('la2', UInt32(),),
                # 60-63	Lo2 — longitude of last grid point (see Note 1)
                ('lo2', UInt32(),),
                # 64-67	Di — i direction increment (see Notes 1 and 5)
                ('di', UInt32(),),
                # 68-71	Dj — j direction increment (see Note 1 and 5)
                ('dj', UInt32(),),
                # 72	Scanning mode (flags — see Flag Table 3.4 and Note 6)
                ('scanning_mode', UInt8(),),
                # 73-nn List of number of points along each meridian or parallel
                # (These octets are only present for quasi-regular grids as described in notes 2 and 3)
            )
        }

    @property
    def fields(self):
        return (
            ('source', UInt8(),),
            ('data_point_count', UInt32(),),
            ('point_count_octets', UInt8(),),
            ('point_count_interpretation', UInt8(),),
            ('grid_template', Template(self.grid_templates),),
        )
    
    @property
    def la1(self) -> int:
        return self.values['la1']
    
    @property
    def lo1(self) -> int:
        return self.values['lo1']
    
    @property
    def la2(self) -> int:
        return self.values['la2']
    
    @property
    def lo2(self) -> int:
        return self.values['lo2']

    @property
    def ni(self):
        return self.values['ni']

    @property
    def nj(self):
        return self.values['nj']


class Section4(Section):

    @property
    def product_templates(self):
        common = (
            # Parameter category
            ('category', UInt8(),),
            # Parameter number
            ('parameter_number', UInt8(),),
            # Type of generating process
            ('type_of_generating', UInt8(),),
            # Background generating process identifier 
            ('generating_process_identifier', UInt8(),),
            # Analysis or forecast generating process identified 
            ('analysis', UInt8(),),
            # Hours after reference time data cutoff
            ('hours_time_data_cutoff', UInt16(),),
            # Minutes after reference time data cutoff
            ('minutes_time_data_cutoff', UInt8(),),
            # Indicator of unit of time range
            ('time_range_unit', UInt8(),),
            # Forecast time in units defined by octet 18
            ('forecast_time', UInt32(),),
            # Type of first fixed surface
            ('first_fixed_surface', UInt8(),),
            # Scale factor of first fixed surface
            ('first_scale_factor_surface', UInt8(),),
            # Scaled value of first fixed surface
            ('first_scaled_value_surface', UInt32(),),
            # Type of second fixed surfaced
            ('second_fixed_surfaced', UInt8(),),
            # Scale factor of second fixed surface
            ('second_scale_factor_surface', UInt8(),),
            # Scaled value of second fixed surface
            ('second_scaled_value_surface', UInt32(),),
        )
        return {
            0: (
                *common,
            ),
            8: (
                *common,
                # Year  ― Time of end of overall time interval
                ('year', UInt16(),),
                # Month  ― Time of end of overall time interval
                ('month', UInt8(),),
                # Day  ― Time of end of overall time interval
                ('day', UInt8(),),
                # Hour  ― Time of end of overall time interval
                ('hour', UInt8(),),
                # Minute  ― Time of end of overall time interval
                ('minute', UInt8(),),
                # Second  ― Time of end of overall time interval
                ('second', UInt8(),),
            )
        }

    @property
    def fields(self):
        return (
            # Number of coordinate values after template (See note 1 below)
            ('coordinate_values', UInt16(),),
            # Product definition template number
            ('product_template', Template(self.product_templates),),
        )
    
    @property
    def year(self) -> int:
        return self.values.get('year', 0)
    
    @property
    def month(self) -> int:
        return self.values.get('month', 0)
    
    @property
    def day(self) -> int:
        return self.values.get('day', 0)
    
    @property
    def hour(self) -> int:
        return self.values.get('hour', 0)


class ErrorS5BitsValue(Exception): pass


class Section5(Section):

    @property
    def data_templates(self):
        return {
            0: (
                ('reference', Float32(),),
                ('binary_scale', UInt16(),),
                ('decimal_scale', UInt16(),),
                ('bits', UInt8(),),
                ('type', UInt8(),),
            ),
            40: (
                ('reference', Float32(),),
                ('binary_scale', UInt16(),),
                ('decimal_scale', UInt16(),),
                ('bits', UInt8(),),
                ('type', UInt8(),),
                # Type of Compression used
                ('compression', UInt8(),),
                # Target compression ratio, M:1 
                ('compression_ratio', UInt8(),),
            )
        }

    @property
    def fields(self):
        return (
            ('points_number', UInt32(),),
            ('data_template', Template(self.data_templates),),            
        )

    @property
    def points_number(self) -> int:
        return self.values['points_number']
    
    @property
    def data_template(self) -> int:
        return self.values['data_template']
    
    @property
    def reference(self):
        return self.values['reference']
    
    @property
    def binary_scale(self):
        return self.values['binary_scale']
    
    @property
    def decimal_scale(self):
        return self.values['decimal_scale']
    
    @property
    def bits(self):
        return self.values['bits']
    
    async def load(self):
        result = await super().load()

        if self.bits % 8 != 0:
            raise ErrorS5BitsValue()
        
        return result


def power(x: float, y: int):
    if y < 0:
        y = -y
        x = 1. / x

    value = 1.

    while y:
        if y & 1:
            value *= x
        
        x = x * x
        y >>= 1

    return value


class Section7:

    def __init__(self, fp: GRIB2File, section_len: int, s5: Section5):
        self._fp = fp

        self._reference = s5.reference
        self._decimal_scale = power(10., -s5.decimal_scale)
        self._binary_scale = power(2., s5.binary_scale)

        total_bits = s5.bits * s5.points_number
        self._size = total_bits // 8
        
        self._bits = s5.bits
        self._points_number = s5.points_number

        self._step = self._bits // 8
        self._c = 0

    async def load(self):
        self._data = await self._fp.read(self._size)

    def cunks(self):
        # TODO: Fix it, can be lazy
        if self._bits == 0:
            return

        while self._c < self._points_number:
            offset = self._c * self._step
            r_value = self._data[offset:offset+self._step]
            value = int.from_bytes(r_value, 'big')
            v = self._decimal_scale * ( self._reference + value * self._binary_scale )
            yield v
            self._c += 1

    def next(self):
        if self._bits == 0:
            return
        
        if self._c < self._points_number:
            offset = self._c * self._step
            r_value = self._data[offset:offset+self._step]
            value = int.from_bytes(r_value, 'big')
            v = self._decimal_scale * ( self._reference + value * self._binary_scale )
            self._c += 1
            return v


class GRIB2Message:

    @property
    def s0(self):
        return self._s0
    
    @property
    def s1(self):
        return self._s1
    
    @property
    def s3(self):
        return self._s3

    @property
    def s4(self):
        return self._s4

    @property
    def s5(self):
        return self._s5
    
    @property
    def s7(self):
        return self._s7

    def __init__(self, fp: GRIB2File):
        self._fp = fp

    async def load(self):
        self._s0 = Section0(self._fp)
        await self._s0.load()
        while True:
            section_len, section_number = await self._read_section_header()
            if section_number == 1:
                self._s1 = Section1(self._fp, section_len)
                await self._s1.load()
            elif section_number == 3:
                self._s3 = Section3(self._fp, section_len)
                await self._s3.load()
            elif section_number == 4:
                self._s4 = Section4(self._fp, section_len)
                await self._s4.load()
            elif section_number == 5:
                self._s5 = Section5(self._fp, section_len)
                await self._s5.load()
            elif section_number == 7:
                self._s7 = Section7(fp=self._fp, section_len=section_len, s5=self._s5)
                await self._s7.load()
                break
            else:
                self._fp.seek(self._fp.tell() + section_len)

    async def _read_section_header(self):
        r_section_len = await self._fp.read(4)
        section_len = int.from_bytes(r_section_len, 'big')
        r_section_number = await self._fp.read(1)
        section_number = int.from_bytes(r_section_number, 'big')
        return section_len-5, section_number


class GRIB2:

    def __init__(self, fp: GRIB2File):
        fp.seek(0)
        self._fp = fp

    async def messages(self):
        while True:
            start = await self._fp.read(4)
            if start == b'GRIB':
                m = GRIB2Message(self._fp)
                await m.load()
                yield m
                end = await self._fp.read(4)
                if end != b'7777':
                    break
            else:
                break   
