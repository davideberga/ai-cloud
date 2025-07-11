from pyspark.sql import SparkSession
import os, sys
import shutil


class MyUtil:

    @staticmethod
    def merge_files(root_folder, merged_file):
        with open(merged_file, 'w') as outfile:
            for fname in os.listdir(root_folder):
                if not fname.startswith("_"):
                    with open(os.path.join(root_folder, fname)) as infile:
                        shutil.copyfileobj(infile, outfile)

    @staticmethod
    def delete_path(path: str):
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.isfile(path):
            os.remove(path)
