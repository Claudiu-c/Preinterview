import pandas as pd
import numpy as np
import os
import random


def read_csv(file_path):
    # Reads a CSV file and returns a DataFrame
    try:
        data = pd.read_csv(
            file_path, header=None, names=["Stock-ID", "Timestamp", "Stock price value"]
        )
        data["Timestamp"] = pd.to_datetime(data["Timestamp"], dayfirst=True)
        print(f"Successfully read {file_path}")
        return data
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None


def get_sample_data(data):
    # Samples 30 consecutive data points from the DataFrame
    try:
        start_idx = random.randint(0, len(data) - 30)
        sample_data = data.iloc[start_idx : start_idx + 30].reset_index(drop=True)
        print(f"Sampled data from index {start_idx}")
        return sample_data
    except Exception as e:
        print(f"Error sampling data: {e}")
        return None


def identify_outliers(sample_data):
    # Identifies outliers in the sampled data based on a threshold
    mean_price = sample_data["Stock price value"].mean()
    std_dev = sample_data["Stock price value"].std()
    threshold = mean_price + 2 * std_dev

    print(f"Mean price: {mean_price}, Std dev: {std_dev}, Threshold: {threshold}")

    outliers = sample_data[sample_data["Stock price value"] > threshold].copy()
    outliers["Mean of 30 data points"] = mean_price
    outliers["% Deviation"] = (
        (outliers["Stock price value"] - mean_price) / mean_price
    ) * 100

    print(f"Identified {len(outliers)} outliers")
    print(outliers)
    return outliers


def save_outliers(outliers, output_file):
    # Saves the outliers DataFrame to a CSV file
    try:
        outliers.to_csv(output_file, index=False)
        print(f"Outliers saved to {output_file}")
    except Exception as e:
        print(f"Error saving outliers: {e}")


def process_file(file_path):
    # Processes a single CSV file to identify and save outliers
    print(f"Processing file: {file_path}")
    data = read_csv(file_path)
    if data is not None and len(data) >= 30:
        sample_data = get_sample_data(data)
        if sample_data is not None:
            outliers = identify_outliers(sample_data)
            if not outliers.empty:
                output_file = f"outliers_{os.path.basename(file_path)}"
                save_outliers(outliers, output_file)
            else:
                output_file = f"no_outliers_{os.path.basename(file_path)}"
                save_outliers(sample_data, output_file)
                print(f"No outliers found in {file_path}")
        else:
            print(f"Could not sample data from {file_path}")
    else:
        print(f"Not enough data in {file_path}")


def main(input_directory, num_files_to_process):
    # Main function to process multiple CSV files in the given directory
    try:
        files = []
        for root, dirs, filenames in os.walk(input_directory):
            for filename in filenames:
                if filename.endswith(".csv"):
                    files.append(os.path.join(root, filename))

        files_to_process = files[:num_files_to_process]

        for file_path in files_to_process:
            process_file(file_path)

    except Exception as e:
        print(f"Error processing files: {e}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Identify outliers in stock price data."
    )
    parser.add_argument("input_directory", help="Directory containing input CSV files")
    parser.add_argument(
        "num_files_to_process", type=int, help="Number of files to process"
    )
    args = parser.parse_args()

    main(args.input_directory, args.num_files_to_process)
