import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

SOURCE = ROOT / "data/raw/UNSW_NB15_training-set.csv"
OUTPUT = ROOT / "data/broken/unsw_corrupted.csv"

def main():
    with SOURCE.open(
        newline="",
        encoding="utf-8-sig",
    ) as file:
        reader = csv.DictReader(file)

        fieldnames = reader.fieldnames
        rows = [next(reader) for _ in range(8)]


    #  Record 1: Non-numeric duration
    rows[0]["dur"] = "INVALID"

    #  Record 2: Negative duration
    rows[1]["dur"] = "-5.2"

    #  Record 3: Negative byte count
    rows[2]["sbytes"] = "-100"

    #  Record 4: Invalid classification label
    rows[3]["label"] = "3"

    #  Record 5: Missing protocol
    rows[4]["proto"] = ""

    #  Record 6: Nonfinite duration
    rows[5]["dur"] = "NaN"
    
    #  Record 7: Multiple validation errors
    rows[6]["dur"] = "-10"
    rows[6]["sbytes"] = "INVALID"
    rows[6]["label"] = "5"
    
    #  Record 8: Valid but unusually high traffic
    rows[7]["sbytes"] = "1000"

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames,
        )

        writer.writeheader()
        writer.writerows(rows)
    
    print(f"Created {OUTPUT}")
    print(f"Generated {len(rows)} records")

if __name__ == "__main__":
    main()

