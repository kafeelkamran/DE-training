import argparse
from sample_project import greet

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("name")
    args=parser.parse_args()
    print(greet(args.name))


if __name__=='__main__':
    main()