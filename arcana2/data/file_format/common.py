from .base import FileFormat, Converter
from nipype.interfaces.utility import IdentityInterface
from arcana2.utils.interfaces import (
    ZipDir, UnzipDir, TarGzDir, UnTarGzDir)

class IdentityConverter(Converter):

    requirements = []
    interface = IdentityInterface(['i'])
    input = 'i'
    output = 'i'


class UnzipConverter(Converter):

    interface = UnzipDir()
    mem_gb = 12
    input = 'zipped'
    output = 'unzipped'


class ZipConverter(Converter):

    interface = ZipDir()
    mem_gb = 12
    input = 'dirname'
    output = 'zipped'


class TarGzConverter(Converter):

    interface = TarGzDir()
    mem_gb = 12
    input = 'dirname'
    output = 'zipped'


class UnTarGzConverter(Converter):

    interface = UnTarGzDir()
    mem_gb = 12
    input = 'gzipped'
    output = 'gunzipped'


# General formats
directory = FileFormat(name='directory', extension=None, directory=True)
text = FileFormat(name='text', extension='.txt')
json = FileFormat(name='json', extension='.json')

# Compressed formats
zip = FileFormat(name='zip', extension='.zip')
targz = FileFormat(name='targz', extension='.tar.gz')

standard_formats = [text, json, directory, zip, targz]

# General image formats
gif = FileFormat(name='gif', extension='.gif')
png = FileFormat(name='png', extension='.png')
jpg = FileFormat(name='jpg', extension='.jpg')

# Document formats
pdf_format = FileFormat(name='pdf', extension='.pdf')

# Set Converters
directory.set_converter(zip, UnzipConverter)
directory.set_converter(targz, UnTarGzConverter)
targz.set_converter(directory, TarGzConverter)
zip.set_converter(directory, ZipConverter)
