import time

import numpy as np
import pandas as pd
import requests

headers = {"User-Agent": "PostmanRuntime/7.26.8"}
session = requests.Session()


def get_transaction_df(
    address: str, start_block: int = 0, end_block: int = 999999999
) -> pd.DataFrame:
    url = f"https://api-ropsten.etherscan.io/api?module=account&action=tokentx&address={address}&startblock={str(start_block)}&endblock={str(end_block)}&sort=asc&apikey=K7ST5DC6VP2Z5ZVWWD1IB3JDB5AHIEV274"

    while True:
        res = session.get(url, headers=headers)
        if res.ok:
            data = res.json()
            if data["status"] == "1":
                break
        time.sleep(1)
    df = pd.DataFrame(data["result"])
    return df


def get_bktc_transaction_df(
    address: str, start_block: int = 0, end_block: int = 999999999
) -> pd.DataFrame:
    df = get_transaction_df(
        address=address, start_block=start_block, end_block=end_block
    )
    df = df[df["tokenSymbol"] == "BKTC"]
    return df


def manipulate_transaction_df(df: pd.DataFrame) -> pd.DataFrame:
    # prepare for result
    df["value"] = df["value"].str[:-18].astype(np.uint64)
    df = df[["hash", "from", "to", "value"]]
    df = df.rename(
        columns={
            "hash": "Tx hash",
            "from": "From (address)",
            "to": "To (address)",
            "value": "Amount transfer",
        }
    )
    df = df.set_index("Tx hash")
    return df


def main(address: str, start_block: int = 0) -> pd.DataFrame:
    tnx_df = get_bktc_transaction_df(address=address, start_block=start_block)
    tnx_df = tnx_df[tnx_df["from"] == address]

    df_list = [tnx_df]

    for adr in tnx_df["to"].unique():
        tmp_df = main(adr)
        if not tmp_df.empty:
            df_list.append(tmp_df)

    if df_list:
        out_df = pd.concat(df_list)
        out_df = out_df.drop_duplicates(subset=["hash"])
        return out_df
    else:
        return pd.DataFrame()


def get_balance(address: str) -> int:
    df = get_bktc_transaction_df(address)
    return (df["value"].str[:-18].astype(np.uint64)).sum()


def get_balance_df(address_list) -> pd.DataFrame:
    bal_dict = dict()
    for addr in address_list:
        bal_dict[addr] = get_balance(addr)
    df = pd.Series(bal_dict).to_frame(name="Balance")
    return df


if __name__ == "__main__":
    init_address = "0xEcA19B1a87442b0c25801B809bf567A6ca87B1da".lower()
    transaction_df = main(init_address)
    transaction_df = manipulate_transaction_df(transaction_df)
    print("Out put 1:")
    print(transaction_df)

    address_list = set(transaction_df["From (address)"]).union(
        set(transaction_df["To (address)"])
    )

    balance_df = get_balance_df(address_list)
    print("Out put 2:")
    print(balance_df)
