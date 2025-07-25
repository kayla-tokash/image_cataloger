from datetime import datetime
from os import listdir
from os.path import isfile, isdir, join
import sqlite3


def list_files_in_directory(path, files_only, dir_only):
    if files_only and dir_only:
        raise RuntimeError("Cannot set file_only and dir_only at the same time")
    elif files_only:
        return [f for f in listdir(path) if isfile(join(path, f))]
    elif dir_only:
        return [f for f in listdir(path) if isdir(join(path, f))]
    # Or return everything
    return listdir()


def filter_on_extensions(files, *extensions):
    return [f for f in files if f.split('.') in extensions]


class CatalogDatabase:
    connection = None
    cursor = None
    catalog_name = None

    def __init__(self, catalog_name = "image_catalog.db"):
        self.catalog_name = catalog_name
        if isfile(catalog_name):
            self.connect_to_catalog()
        else:
            self.create_catalog()

    def connect_to_catalog(self):
        # Standard connection to sqlite database
        self.connection = sqlite3.connect(self.catalog_name)
        self.cursor = self.connection.cursor()

    def create_catalog(self):
        # Create the database using schema
        self.connect_to_catalog()
        self.cursor.execute("CREATE TABLE images(file_path, date, tags)")
        pass

    def add_file_to_catalog(self, file_path, *tags):
        list_of_tags = ','.join(tags)
        date_string = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        self.cursor.execute(f"INSERT INTO images VALUES ('{file_path}', '{date_string}', '{list_of_tags}')")
        self.connection.commit()

    def get_all_files(self):
        pass

    def get_tags_for_files(self, file_path):
        pass

