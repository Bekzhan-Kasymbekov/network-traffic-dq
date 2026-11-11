import csv
from collections import Counter
from pathlib import Path

DATA_PATH = (
    Path(__file__).resolve().parent.parent
    / "data"
    / "raw"
    / "UNSW_NB15_training-set.csv"
)

def main():
    with DATA_PATH.open(newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)

        print("Columns:")
        print(reader.fieldnames)

        total_rows = 0
        protocols = Counter()
        labels = Counter()
        missing_values = Counter()

        for row in reader:
            total_rows += 1

            protocols[row["proto"]] += 1
            labels[row["label"]] += 1

            for column, value in row.items():
                if value is None or value.strip() == "":
                    missing_values[column] += 1

    print(f"\nTotal records: {total_rows}")

    print("\nTop protocols:")
    for protocol, count in protocols.most_common(10):
        print(f" {protocol}: {count}")

    print("\nLabels:")
    for label, count in labels.items():
        print(f" {label}: {count}")

    print("\nMissing values:")
    for column, count in missing_values.items():
        print(f" {column}: {count}")

if __name__ == "__main__":
    main()
