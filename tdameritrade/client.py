import os
import requests
import pandas as pd
from .urls import (
    ACCOUNTS,
    INSTRUMENTS,
    QUOTES,
    SEARCH,
    HISTORY,
    OPTIONCHAIN,
    MOVERS,
    WATCHLISTS,
    TOKEN,
)


class TDClient(object):
    def __init__(self, refresh_token=None, accountIds=None, clientId=None):
        self._access_token = os.getenv("ACCESS_TOKEN", "")
        self._refresh_token = refresh_token or os.getenv("REFRESH_TOKEN", "")
        self._client_id = clientId or os.getenv("CLIENT_ID", "")
        self.accountIds = accountIds or []

    def _headers(self):
        return {"Authorization": "Bearer " + self._access_token}

    def _refreshAccessToken(self):
        data = {
            "grant_type": "refresh_token",
            "client_id": self._client_id,
            "refresh_token": self._refresh_token,
        }
        response = requests.post(TOKEN, data=data)
        token = response.json()["access_token"]
        self._access_token = token
        os.environ["ACCESS_TOKEN"] = token
        return token

    def get(self, url, params=None):
        response = requests.get(url, headers=self._headers(), params=params)
        if response.status_code == 401:
            self._refreshAccessToken()
            response = requests.get(url, headers=self._headers(), params=params)
        return response

    def accounts(self):
        ret = {}
        if self.accountIds:
            for acc in self.accountIds:
                resp = self.get(ACCOUNTS + str(acc))
                if resp.status_code == 200:
                    ret[acc] = resp.json()
                else:
                    raise Exception(resp.text)
        else:
            resp = self.get(ACCOUNTS)
            if resp.status_code == 200:
                for account in resp.json():
                    ret[account["securitiesAccount"]["accountId"]] = account
            else:
                raise Exception(resp.text)
        return ret

    def accountsDF(self):
        return pd.io.json.json_normalize(self.accounts())

    def search(self, symbol, projection="symbol-search"):
        return self.get(
            SEARCH, params={"symbol": symbol, "projection": projection}
        ).json()

    def searchDF(self, symbol, projection="symbol-search"):
        ret = []
        dat = self.search(symbol, projection)
        for symbol in dat:
            ret.append(dat[symbol])
        return pd.DataFrame(ret)

    def fundamental(self, symbol):
        return self.search(symbol, "fundamental")

    def fundamentalDF(self, symbol):
        return self.searchDF(symbol, "fundamental")

    def instrument(self, cusip):
        return self.get(INSTRUMENTS + str(cusip)).json()

    def instrumentDF(self, cusip):
        return pd.DataFrame(self.instrument(cusip))

    def quote(self, symbol):
        return self.get(QUOTES, params={"symbol": symbol.upper()}).json()

    def quoteDF(self, symbol):
        x = self.quote(symbol)
        return pd.DataFrame(x).T.reset_index(drop=True)

    def history(self, symbol):
        return self.get(HISTORY % symbol).json()

    def historyDF(self, symbol):
        x = self.history(symbol)
        df = pd.DataFrame(x["candles"])
        df["datetime"] = pd.to_datetime(df["datetime"], unit="ms")
        return df

    def options(self, symbol):
        return self.get(OPTIONCHAIN, params={"symbol": symbol.upper()}).json()

    def optionsDF(self, symbol):
        ret = []
        dat = self.options(symbol)
        for date in dat["callExpDateMap"]:
            for strike in dat["callExpDateMap"][date]:
                ret.extend(dat["callExpDateMap"][date][strike])
        for date in dat["putExpDateMap"]:
            for strike in dat["putExpDateMap"][date]:
                ret.extend(dat["putExpDateMap"][date][strike])

        df = pd.DataFrame(ret)
        for col in (
            "tradeTimeInLong",
            "quoteTimeInLong",
            "expirationDate",
            "lastTradingDay",
        ):
            df[col] = pd.to_datetime(df[col], unit="ms")
        return df

    def movers(self, index, direction="up", change_type="percent"):
        return self.get(
            MOVERS % index, params={"direction": direction, "change_type": change_type}
        )

    def watchlists(self, accountId=None, watchlistId=None):
        accId = str(accountId) if accountId else ""
        wlId = "/" + str(watchlistId) if watchlistId else ""
        response = self.get(WATCHLISTS % (accId, wlId))
        return response.json()

    def watchlistsDF(self, accountId=None, watchlistId=None):
        return pd.io.json.json_normalize(self.watchlists(accountId, watchlistId))
