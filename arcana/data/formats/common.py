from abc import ABCMeta
from arcana.core.data.format import FileGroup, BaseFile, BaseDirectory
from arcana.core.mark import converter
from arcana.tasks.archive import (
    create_tar, extract_tar, create_zip, extract_zip)


# General formats
class Text(BaseFile):
    ext = 'txt'

class Csv(BaseFile):
    ext = 'csv'

class Tsv(BaseFile):
    ext = 'tsv'

class TextMatrix(BaseFile):
    ext = 'mat'

class RFile(BaseFile):
    ext = 'rData'

class MatlabMatrix(BaseFile):
    ext = 'mat'


# Compressed formats
class Zip(BaseFile):
    ext = 'zip'

    @classmethod
    @converter(FileGroup)
    def archive(cls, fs_path):
        node = create_zip(
            in_file=fs_path,
            compression='gz')
        return node, node.lzout.out_file

class Gzip(BaseFile):
    ext = 'gz'

    @classmethod
    @converter(FileGroup)
    def archive(cls, fs_path):
        raise NotImplementedError

class Tar(BaseFile):
    ext = 'tar'

    @classmethod
    @converter(FileGroup)
    def archive(cls, fs_path):
        node = create_tar(
            in_file=fs_path,
            compression='gz')
        return node, node.lzout.out_file    

class TarGz(Tar, Gzip):
    ext = 'tar.gz'

    @classmethod
    @converter(FileGroup)
    def archive(cls, fs_path):
        node = create_tar(
            in_file=fs_path,
            compression='gz')
        return node, node.lzout.out_file


class Directory(BaseDirectory):

    @classmethod
    @converter(Zip)
    def unzip(cls, fs_path):
        node = extract_zip(
            in_file=fs_path)
        return node, node.lzout.out_file

    @classmethod
    @converter(Tar)
    def untar(cls, fs_path):
        node = extract_tar(
            in_file=fs_path)
        return node, node.lzout.out_file

    @classmethod
    @converter(TarGz)
    def untargz(cls, fs_path):
        node = extract_tar(
            in_file=fs_path)
        return node, node.lzout.out_file


# Hierarchical text files

class HierarchicalText(BaseFile):
    pass

class Json(HierarchicalText):
    ext = 'json'

class Yaml(HierarchicalText):
    ext = 'yml'    


standard_formats = [Text, Directory, Zip, Tar, TarGz]

# General image formats
class ImageFile(BaseFile, metaclass=ABCMeta):
    pass

class Gif(ImageFile):
    ext = 'gif'

class Png(ImageFile):
    ext = 'png'

class Jpeg(ImageFile):
    ext = 'jpg'

# Document formats
class Document(BaseFile, metaclass=ABCMeta):
    pass

class Pdf(Document):
    ext = 'pdf'
