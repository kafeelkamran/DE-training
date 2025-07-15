import argparse
import pandas as pd
import logging
import os

logging.basicConfig(level = logging.INFO,format = '%(asctime)s [%(levelname)s] %(message)s')

def convert_csv_to_parquet(input_file, output_file):
    logging.info(f"Reading CSV:{input_file}")
    df = pd.read_csv(input_file)
    logging.info(f"Writing to Parquet: {output_file}")
    df.to_parquet(output_file, index = False)
    logging.info("Conversion complete.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='convert csv to parquet')
    parser.add_argument('--infile', required=True, help = 'Input csv path')
    parser.add_argument('--outfile', required=True, help = 'Output Parquet Path')
    args = parser.parse_args()

    if not os.path.exists(args.infile):
        logging.error("Input file not found")
    else:
        convert_csv_to_parquet(args.infile, args.outfile)