from puller.puller_v4 import long_int_pull
import pandas as pd



print('running...')

utilities = ["PSEG", "RGE", "NYSEG", "COMED", "PECO", "PPL", "BGE", "JCPL", "METED", "OHIO", "Cleveland",
                 "Toledo", "AEP", "PEPCO", "PSEG_NG", "Nat_Grid_NY", "Nat_Grid_LI"]  + ["ALL"]

code = [i for i in range(0,len(utilities)-1)]+[50]


d = {'Code': code,
     'Utility': utilities
     }

df = pd.DataFrame(data=d).set_index('Utility')


def run():
    lookback = int(input('Lookback period(days): '))

    while True:
        try:
            util_pull(lookback)
        except Exception:
            util_pull(lookback)
        else:
            util_pull(lookback)


def util_pull(lookback):
    print('CURRENT LOOKBACK: %s'%(lookback))
    print(df)
    utility = int(input('Utility code:'))

    if utility == 50:
        for n in utilities[:-1]:
            try:
                long_int_pull(n,lookback)
            except Exception as e:
                print(e)

    else:
        try:
            long_int_pull(df.Code[df.Code == utility].index[0],lookback)
        except Exception as e:
            print(e)
