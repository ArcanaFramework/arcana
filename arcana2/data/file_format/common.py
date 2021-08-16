from .base import FileFormat, Converter
from nipype.interfaces.utility import IdentityInterface
from arcana2.tasks.archive import (
    create_tar, extract_tar, create_zip, extract_zip)


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
directory.set_converter(zip, extract_zip)
directory.set_converter(targz, extract_zip)
targz.set_converter(directory, create_tar, compression='gz')
zip.set_converter(directory, extract_zip)
