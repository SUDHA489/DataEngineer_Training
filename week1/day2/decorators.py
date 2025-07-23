import argparse
parser=argparse.ArgumentParser(description="processing data...")
parser.add_argument("name",type=str)
parser.add_argument("age",type=int)
args=parser.parse_args()


# A basic decorator
def my_decorator(func):
    def wrapper(*args,**kwargs):
        print(*args)
        print(**kwargs)
        print("Something before the function runs.")
        func(*args,**kwargs)
        print("Something after the function runs.")
    return wrapper

# Applying the decorator
@my_decorator
def say_hello(name,age):
    print("Hello,my name is ",name,".i am",age,"years old")

# Calling the decorated function

say_hello(args.name,args.age)