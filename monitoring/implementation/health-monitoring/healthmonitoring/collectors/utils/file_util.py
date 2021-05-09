'''
Created on 11-May-2020

@author: deerakum
'''
import os
import csv
from pathlib import Path


class FileUtils:

    @staticmethod
    def get_latest_file(file_path):
        dir_path = Path(file_path)
        files_list = list(dir_path.glob("**/*"))
        time, file_path = max((f.stat().st_mtime, f) for f in files_list)
        return file_path

    @staticmethod
    def get_files_count_in_dir(path):
        return len(FileUtils.get_files_in_dir(path))

    @staticmethod
    def get_files_in_dir(dir_name, pattern="*"):
        dir_pattern = Path(dir_name)
        return list(dir_pattern.glob(pattern))

    @staticmethod
    def get_file_size_in_mb(filename):
        return round(Path(filename).stat().st_size / 1024 / 1024, 4)

    @staticmethod
    def get_dir_name(path):
        return path.parent

    @staticmethod
    def get_files_by_modtime(path):
        return sorted(Path(path).iterdir(), key=os.path.getmtime, reverse=True)

    @staticmethod
    def exists(file_path):
        return Path(file_path).exists()

    @staticmethod
    def read_file(file_path):
        return Path(file_path).read_text()

    @staticmethod
    def readlines(file_path):
        return Path(file_path).read_text().splitlines()

    @staticmethod
    def get_line_count(file_path):
        if FileUtils.exists(file_path):
            return len(Path(file_path).read_text().splitlines())


class CsvUtils:

    @staticmethod
    def read_csv(file_path):
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            return list(reader)

    @staticmethod
    def read_csv_as_dict_output(file_path):
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            return reader.fieldnames, list(reader)
