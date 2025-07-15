import argparse
from employee_project import greet
import pandas as pd

def clean_data(input_file):
    df = pd.read_csv('Employee.csv')
    df.fillna(0,inplace=True)
    df.to_csv(input_file)
    return

def filter_engineers(input_file,output_file):
    df = pd.read_csv('Employee.csv')
    df.fillna(0,inplace=True)
    filtered_df = df[df['Department']=='Engineering']
    filtered_df.to_csv(output_file,index=False)

def main():
    parser = argparse.ArgumentParser(description="segments for employee data")
    parser.add_argument("--input_file",required=True)
    args=parser.parse_args("--output_file",required=True)
    args=parser.parse_args()

if __name__=='__main__':
    main()