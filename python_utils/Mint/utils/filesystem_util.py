import glob
from pathlib import Path

#list all csv files only
csv_files = glob.glob('*.{}'.format('csv'))
csv_files

class FileSystemUtil:

    def __init__(self, read_file=False, write_file=False,dir_path=False,file_format=False):
        self.read_file = read_file
        self.write_file = write_file
        self.dir_path = dir_path
        self.file_format = file_format
        print(self.dir_path)
    
    def get_file_list(self):
        return glob.glob('{d}/*.{f}'.format(d=self.dir_path,f=self.file_format))
    

    def get_filtered_files_list(self):
        p = Path(self.dir_path)
        filtered = [x for x in p.glob("**/*") if not x.name.startswith("temp")]
