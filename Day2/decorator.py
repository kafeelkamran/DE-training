import argparse

def log_time(func):
    print("I'm here logtime")
    print(func)
    def wrapper(*args,**kwargs):
        print(*args)
        print(**kwargs)
        print("I'm from logtime=>wrapper")
        result = func(*args,**kwargs)
        print("Function ended")
        return result
    return wrapper

@log_time
def process():
    print("I'm printing from the process")
    print()

process()

# def sample():
#     print("hi")

# log_time(sample())

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Process some data...")
    parser.add_argument("--name", type = str)
    parser.add_argument("--age",type = int)

    args = parser.parse_args()
    print(args.name)
    print(args.age)
    process()
