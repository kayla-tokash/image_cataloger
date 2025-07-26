#### TODO Separate the indexer to a new file

from os import listdir
from os.path import isfile, isdir, join
import ollama

# TODO asynchronously index files
def index_files(path, *extensions):
    files, directories = list_directories_and_files_in_path(path)
    if len(files) == 0 and len(directories) == 0:
        return []
    filtered_files = [f"{path}/{f}" for f in filter_on_extensions(files, *extensions)]
    for directory in directories:
        filtered_files.extend(
            [f for f in index_files(f"{path}/{directory}", *extensions)])
    return filtered_files


def get_file_tags_from_ai(image_file_path):
    # Use vision enabled AI, and file metadata to help categorize the images
    # https://ollama.com/library/llava

    # Tags vary a lot because AI is not deterministic

    # The AI "llava" usually gives short plaintext description of the image,
    # followed by a line "Tags:" and a bulleted list of "tags" that vary in format.

    # Format tags to the same letter casing, and remove symbols and spaces might be
    # the best way to make it consistent
    result = ollama.chat(
        model='llava',
        messages=[{
                'role': 'user',
                'content': 'create a list of tags that describe this image:',
                'images': [image_file_path]
        }]
    )

    tags = []
    for line in result['message']['content'].split('\n'):
        cleaned_tag = line.replace('-','').replace('#','').replace(' ','').upper()
        if len(cleaned_tag) > 0:
            tags.append(cleaned_tag)

    print(f"Tags for \"{image_file_path}\": {tags}")

    return tags

def get_file_tags_from_metadata(file_list):
    pass


def list_directories_and_files_in_path(path):
    contents = listdir(path)
    return [f for f in contents if isfile(join(path, f))], [f for f in contents if isdir(join(path, f))]


def filter_on_extensions(files, *extensions):
    return [f for f in files if f.split('.')[-1] in extensions]

# TODO verify MIME type

#### TODO Separate the database to a new file

import sqlite3
import hashlib
from datetime import datetime
# from os.path import isfile


class CatalogDatabase:
    connection = None
    cursor = None
    catalog_name = None

    STATUS_ANY = -1
    STATUS_NEW = 0
    STATUS_CATALOGED = 1
    STATUS_SORTED = 2
    STATUS_SKIPPED = 3
    STATUS_DUPLICATE = 3

    def __init__(self, catalog_name = "image_catalog.db"):
        self.catalog_name = catalog_name
        if isfile(self.catalog_name):
            self.connect_to_catalog()
        else:
            self.create_catalog()


    @staticmethod
    def sha256sum(filename):
        with open(filename, 'rb', buffering=0) as f:
            return hashlib.file_digest(f, 'sha256').hexdigest()


    def connect_to_catalog(self):
        # Standard connection to sqlite database
        self.connection = sqlite3.connect(self.catalog_name)
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.cursor = self.connection.cursor()


    def create_catalog(self):
        # Create the database using schema
        self.connect_to_catalog()
        # Create a table of tags
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS tags(
                tag_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, 
                tag_name VARCHAR NOT NULL
            )""")
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS images(
                file_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, 
                file_path VARCHAR NOT NULL,
                date VARCHAR, 
                hashsum VARCHAR NOT NULL, 
                status INTEGER DEFAULT {self.STATUS_NEW} NOT NULL
            )""")
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS file_tags(
                file_id INTEGER,
                tag_id INTEGER,
                CONSTRAINT reference_file_id
                    FOREIGN KEY(file_id) 
                    REFERENCES images(file_id)
                    ON DELETE CASCADE, 
                CONSTRAINT reference_tag_id
                    FOREIGN KEY(tag_id) 
                    REFERENCES tags(tag_id)
                    ON DELETE CASCADE
            )""")


    def mark_file_status(self, status, file_path = None, hashsum = None):
        assert file_path is not None or hashsum is not None
        if file_path and hashsum:
            self.cursor.execute(f"""
                UPDATE images 
                    SET status = {status} 
                    WHERE images.file_path = '{file_path}' 
                    AND images.hashsum = '{hashsum}'
                """)
        elif file_path:
            self.cursor.execute(f"""
                UPDATE images 
                    SET status = {status} 
                    WHERE images.file_path = '{file_path}' 
                """)
        elif hashsum:
            self.cursor.execute(f"""
                UPDATE images 
                    SET status = {status} 
                    WHERE images.hashsum = '{hashsum}'
                """)
        self.connection.commit()


    def catalog_file_is_valid(self, file_path):
        # Check if the file exists
        if not isfile(file_path):
            return False
        # Check the hashsum, we don't care about the other information for this check
        _, _, _, hashsum, _ = self.get_files(file_path)
        hashsum_on_disk = CatalogDatabase.sha256sum(file_path)
        if hashsum != hashsum_on_disk:
            return False
        return True


    def add_file_to_catalog(self, file_path):
        date_string = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        hashsum = CatalogDatabase.sha256sum(file_path)
        escaped_file_path = file_path.replace("'","''")
        script = f"""
            INSERT INTO images (
                    file_path,
                    date,
                    hashsum
                )
                VALUES (
                    '{escaped_file_path}', 
                    '{date_string}', 
                    '{hashsum}'
                )
            """
        try:
            self.cursor.execute(script)
        except sqlite3.OperationalError as e:
            print(e)
            print(f"Script: {script}")
        self.connection.commit()


    def add_tags_to_catalog(self, *tags):
        assert len(tags) > 0
        commit = False
        for tag in tags:
            count = self.cursor.execute(f"SELECT count(*) FROM tags WHERE tag_name = '{tag}'").fetchone()[0]
            if count == 0:
                self.cursor.execute(f"INSERT INTO tags (tag_name) VALUES ('{tag}')")
                commit = True
        if commit:
            self.connection.commit()


    def get_files(self, file_path = None, hashsum = None, status = STATUS_NEW):
        if file_path and hashsum:
            return self.cursor.execute(f"""
                SELECT * FROM images 
                    WHERE file_path = '{file_path}'
                    AND hashsum = '{hashsum}'
                    AND status = {status}""")
        elif file_path:
            return self.cursor.execute(f"""
                SELECT * FROM images 
                    WHERE file_path = '{file_path}'
                    AND status = {status}""")
        elif hashsum:
            return self.cursor.execute(f"""
                SELECT * FROM images 
                    WHERE hashsum = '{hashsum}'
                    AND status = {status}""")
        else:
            if status == self.STATUS_ANY:
                return self.cursor.execute("SELECT * FROM images")
            else:
                return self.cursor.execute(f"SELECT * FROM images WHERE status = {status}")


    def get_tags_for_file(self, file_path = None, hashsum = None):
        assert file_path is not None or hashsum is not None
        if file_path and hashsum:
            return self.cursor.execute(f"""
                SELECT tag_name FROM tags
                    INNER JOIN images.file_id ON tags.file_id
                    WHERE images.file_path = '{file_path}'
                    AND images.hashsum = '{hashsum}'
            """)
        elif file_path:
            return self.cursor.execute(f"""
                SELECT tag_name FROM tags
                    INNER JOIN images.file_id ON tags.file_id
                    WHERE images.file_path = '{file_path}'
            """)
        elif hashsum:
            return self.cursor.execute(f"""
                SELECT tag_name FROM tags
                    INNER JOIN images.file_id ON tags.file_id
                    WHERE images.hashsum = '{hashsum}'
            """)


    def add_tags_to_file_in_catalog(self, file_id = None, file_path = None, hashsum = None, *new_tags):
        assert file_path is not None or hashsum is not None or file_id is not None
        if not file_id:
            if file_path and hashsum:
                file_id = self.cursor.execute(
                    f"SELECT file_id FROM images WHERE images.file_path = '{file_path}' AND images.hashsum = '{hashsum}' LIMIT 1")
            elif file_path:
                file_id = self.cursor.execute(
                    f"SELECT file_id FROM images WHERE images.file_path = '{file_path}' LIMIT 1")
            elif hashsum:
                file_id = self.cursor.execute(
                    f"SELECT file_id FROM images WHERE images.hashsum = '{hashsum}' LIMIT 1")
        print(f"New tags: {new_tags}")
        for tag in new_tags:
            print(f"Adding {tag} to file with ID {file_id} (Name: {file_path})")
            tag_count = self.cursor.execute(f"SELECT count(*) FROM tags WHERE tag_name = '{tag}'").fetchone()[0]
            if tag_count == 0:
                print(f"Tag {tag} does not exists, adding")
                self.cursor.execute(f"INSERT INTO tags (tag_name) VALUES ('{tag}')")
                self.connection.commit()
            else:
                print(f"Tag {tag} already exists")
            new_tag_id = self.cursor.execute(f"SELECT tag_id FROM tags WHERE tag_name = '{tag}' LIMIT 1").fetchone()[0]
            print(f"New tag ID is {new_tag_id} for {tag}")
            self.cursor.execute(f"INSERT INTO file_tags (file_id, tag_id) VALUES ({file_id}, {new_tag_id})")
            self.connection.commit()
            print(f"Tag complete!")
        print(f"Done tagging folder!")


    def remove_tags_from_file_in_catalog(self, file_path = None, hashsum = None, *remove_tags):
        assert file_path is not None or hashsum is not None
        file_ids = None
        if file_path and hashsum:
            file_ids = self.cursor.execute(
                f"SELECT file_id FROM images WHERE images.file_path = '{file_path}' AND images.hashsum = '{hashsum}'")
        elif file_path:
            file_ids = self.cursor.execute(
                f"SELECT file_id FROM images WHERE images.file_path = '{file_path}'")
        elif hashsum:
            file_ids = self.cursor.execute(
                f"SELECT file_id FROM images WHERE images.hashsum = '{hashsum}'")
        for file_id in file_ids:
            for tag in remove_tags:
                self.cursor.execute(f"DELETE FROM tags WHERE tags.file_id = {file_id} and tags.tag_name = '{tag}'")
        self.connection.commit()


    def remove_file_from_catalog(self, file_path, hashsum=None):
        self.cursor.execute(f"""
            DELETE FROM images
                WHERE images.file_path = '{file_path}'
                AND images.hashsum = '{hashsum}'
        """)
        self.connection.commit()

if __name__ == "__main__":
    db = CatalogDatabase()
    print("Created catalog database")
    for file_path in index_files("../../../Downloads", "png", "jpg", "bmp"):
        db.add_file_to_catalog(file_path)
        print(f"Added {file_path} to catalog")
        for file_id, _, _, _, _ in db.get_files(file_path):
            new_tags = get_file_tags_from_ai(file_path)
            print(f"New tags: {new_tags}")
            db.add_tags_to_file_in_catalog(file_id, None, None, *new_tags)