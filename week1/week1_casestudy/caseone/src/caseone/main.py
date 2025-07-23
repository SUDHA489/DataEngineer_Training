import argparse
from .cli_tool import ETL


def main():
    parser=argparse.ArgumentParser("taking inputs")
    parser.add_argument("input")
    parser.add_argument("output")
    args=parser.parse_args()
    ETL(args.input,args.output)