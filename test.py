from util   import get_expirations, get_records_by_contract
from sys    import argv
from time   import time


def get_rec_test(
    symbol: str,
    start:  str,
    end:    str
):
    
    recs = get_records_by_contract(symbol, start, end)

    pass


def get_expirations_test(
    symbol: str,
    start:  str,
    end:    str,
    type:   str,
    rule:   str,
    months: str
):

    res     = get_records_by_contract(symbol, start, end)
    months  = int(months)

    for id, recs in res.items():

        res = get_expirations(recs, type, rule, months)

    pass



TESTS = {
    0: get_rec_test,
    1: get_expirations_test
}


if __name__ == "__main__":

    t0      = time()
    test    = int(argv[1])
    args    = argv[2:]

    TESTS[test](*args)

    print(f"elapsed: {time() - t0:0.1f}")