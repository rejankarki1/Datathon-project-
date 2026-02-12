#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path


DEFAULT_CITIES = ["San Marcos", "Austin", "College Station", "Denton"]


def normalize_city(name: str) -> str:
    return " ".join(name.strip().split()).lower()


def load_rows(path: Path):
    with path.open(newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            yield header, row


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Filter Zillow ZORI city data to selected cities."
    )
    parser.add_argument(
        "--input",
        default="ZilloZoriCityRaw.csv",
        help="Path to the raw Zillow ZORI CSV.",
    )
    parser.add_argument(
        "--output",
        default="data/cleaned_zillow_zori_city.csv",
        help="Path to write the cleaned CSV.",
    )
    parser.add_argument(
        "--long",
        action="store_true",
        help="Write a tidy/long dataset with one row per city per date.",
    )
    parser.add_argument(
        "--cities",
        nargs="+",
        default=DEFAULT_CITIES,
        help="City names to keep.",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Handle common misspelling from the request.
    city_map = {"san marcoc": "san marcos"}
    normalized = []
    for city in args.cities:
        key = normalize_city(city)
        normalized.append(city_map.get(key, key))
    target_cities = set(normalized)

    with input_path.open(newline="", encoding="utf-8", errors="replace") as f_in, output_path.open(
        "w", newline="", encoding="utf-8"
    ) as f_out:
        reader = csv.reader(f_in)
        writer = csv.writer(f_out)
        header = next(reader)

        try:
            region_idx = header.index("RegionName")
        except ValueError:
            raise SystemExit("Column 'RegionName' not found in CSV header.")

        if args.long:
            # Assume date columns are everything after CountyName, but fall back to pattern match.
            date_start = None
            try:
                date_start = header.index("CountyName") + 1
            except ValueError:
                pass
            if date_start is None:
                for i, col in enumerate(header):
                    if len(col) == 10 and col[4] == "-" and col[7] == "-":
                        date_start = i
                        break
            if date_start is None:
                raise SystemExit("Could not locate date columns in CSV header.")

            meta_cols = header[:date_start]
            writer.writerow(meta_cols + ["Date", "Value"])
        else:
            writer.writerow(header)

        kept = 0
        total = 0
        for row in reader:
            total += 1
            if region_idx >= len(row):
                continue
            city = normalize_city(row[region_idx])
            if city in target_cities:
                kept += 1
                if args.long:
                    meta = row[:date_start]
                    for i in range(date_start, len(header)):
                        value = row[i] if i < len(row) else ""
                        if value == "":
                            continue
                        writer.writerow(meta + [header[i], value])
                else:
                    writer.writerow(row)

    print(f"Filtered {kept} rows out of {total}. Output: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
