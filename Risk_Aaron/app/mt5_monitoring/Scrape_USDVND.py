import pandas as pd
import numpy as np


def get_usdvnd_price():
    DF_FX = pd.read_html("https://tradingeconomics.com/usdvnd:cur")

    if len(DF_FX) <= 0:
        # Need to send Email to do manually.
        return

    df = DF_FX[0]
    mid_price = df[df["Crosses"] == "USDVND"]["Price"].astype("float")
    ask = float(mid_price + 0.5)
    bid = float(mid_price - 0.5)
    return {"ask": ask, "bid": bid}