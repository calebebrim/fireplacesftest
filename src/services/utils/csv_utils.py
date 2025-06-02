import csv
from datetime import datetime


def from_csv_generator(file_path):
    with open(file_path, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            yield row
    yield {"_end_": True}
