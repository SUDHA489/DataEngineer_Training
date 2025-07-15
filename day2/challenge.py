import pandas as pd
import argparse
parser=argparse.ArgumentParser()
parser.add_argument("dept",type=str)
args=parser.parse_args()

#task1
df=pd.read_csv("employees.csv").fillna(0)
print(df)

#task2 with challenge given
print("after filtering only engineering employees.....")
filterEmployee=df[df['department']==args.dept]
print(filterEmployee)

#task3
filterEmployee.to_csv('engineering_employees.csv',index=True)