from    ast         import literal_eval
import  polars      as      pl
from    util        import  get_expirations, get_records_by_contract
from    sys         import  argv, path
from    time        import  time

path.append("..")

from data.cat_df    import  cat_df


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
    kind:   str,
    rule:   str
):

    res     = get_records_by_contract(symbol, start, end)
    rule    = literal_eval(rule)

    print(rule)
    
    for id, recs in res.items():

        exps = get_expirations(recs, kind, rule)

        print(f"{id}: {' '.join(exps)}")

    pass


def compare_expirations(
    ul_symbol:  str,
    start:      str,
    end:        str,
    opt_class:  str = None
):

    df = cat_df("opts", ul_symbol, start, end)

    if opt_class:

        df = df.filter(pl.col("name") == opt_class)
    
    rows = sorted(df.unique(["name", "expiry"]).rows(), key = lambda r: r[0])

    cons = get_records_by_contract(ul_symbol, start, end)
    exps = []

    for _, recs in cons.items():

        expirations = get_expirations(ul_symbol, recs)

        exps.extend(expirations)

    exps = sorted(exps, key = lambda r: r[0])

    for row in rows:

        print(f"{row[0]}\t{row[1]}")

    for exp in exps:

        print(f"{exp[0]}\t{exp[2]}")

    pass


TESTS = {
    0: get_rec_test,
    1: get_expirations_test,
    2: compare_expirations
}


if __name__ == "__main__":

    t0      = time()
    test    = int(argv[1])
    args    = argv[2:]

    TESTS[test](*args)

    print(f"elapsed: {time() - t0:0.1f}")