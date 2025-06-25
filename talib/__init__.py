class _Placeholder:
    def MACD(self, *args, **kwargs):
        raise NotImplementedError("TA-Lib is not installed")

MACD = _Placeholder().MACD
