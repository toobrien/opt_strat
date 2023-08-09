from    datetime                import  datetime
from    enum                    import  IntEnum
from    json                    import  loads
from    pandas                  import  bdate_range, date_range, DateOffset, Timestamp
from    pandas.tseries.holiday  import  USFederalHolidayCalendar
from    pandas.tseries.offsets  import  BDay, MonthBegin, MonthEnd
import  polars                  as      pl
from    typing                  import  List


CONFIG      = loads(open("./config.json").read())
DATE_FMT    = "%Y-%m-%d"
DB          = pl.read_parquet(CONFIG["db_path"])
EXPIRATIONS = {
    "NG": { 
        "M": ( "UL_EXP-1BD", 1 )
    },
    "CL": {
        "M": ( "UL_EXP-1BD", 1 ),
        "W": ( "MWF", 1 )
    },
    "ZN": {
        "M": ( "-2BD-1FRI", 3 ),
        "W": ( "WF", 3 ),
    }
}
MONTHS      = {
    1:  "F",
    2:  "G",
    3:  "H",
    4:  "J",
    5:  "K",
    6:  "M",
    7:  "N",
    8:  "Q",
    9:  "U",
    10: "V",
    11: "X",
    12: "Z"
}
DAYS_OF_WEEK = {
    "M": 0,
    "T": 1,
    "W": 2,
    "H": 3,
    "F": 4
}
HOLIDAYS = USFederalHolidayCalendar.holidays(start = "1900-01-01", end = "2100-01-01")


class base_rec(IntEnum):

    date    = 0
    month   = 1
    year    = 2
    settle  = 3
    dte     = 4 


def get_monthly_series(
    symbol:     str,        # e.g. ZC, CL, etc.
    type:       str,        # monthly (M) or weekly (W)
    dte:        int,        # time remaining for option strategy
    start:      str,        # data start date yyyy-mm-dd
    end:        str         # data end date   yyyy-mm-dd
):
    
    pass


def get_expirations(
    recs:   List[base_rec],
    kind:   str,
    rule:   str
):

    res         = []
    dates       = [ rec[base_rec.date] for rec in recs ]
    dte         = [ rec[base_rec.dte] for rec in recs ]
    ul_exp      = Timestamp(dates[0]) + DateOffset(days = dte[0])
    bom         = ul_exp + MonthBegin(-1)
    eom         = ul_exp + MonthEnd(0)
    r_name      = rule[0] 
    serial      = rule[1]

    while serial > 0:

        if kind == "M":

            if r_name == "UL_EXP-1BD":

                res.append(ul_exp - BDay())

            elif r_name == "EOM-4BD":
                
                exp = bdate_range(bom, eom, freq = "C", holidays = HOLIDAYS)[-4]

                res.append(exp)
            
            elif r_name == "-2BD-1FRI":

                # first friday prior to the second to last business day of the month;
                # if this is not a business day, then the day prior

                cutoff  = bdate_range(bom, eom, holidays = HOLIDAYS)[-3]
                fri     = date_range(bom, cutoff, freq = "W-FRI")[-1]
                exp     = fri if BDay().is_on_offset(fri) else fri - BDay()

                res.append(exp)

            elif r_name == "25th-(6|7)BD":

                exp = bom + DateOffset(days = 24) - 6 * BDay()
                exp = exp if BDay().is_on_offset(exp) else exp - BDay()

            elif r_name == "10BD":

                exp = bdate_range(bom, eom, freq = "C", holidays = HOLIDAYS)[10]

                res.append(exp)

            elif r_name == "1FRI":

                exp = date_range(bom, eom, freq = "W-FRI")[1]

                res.append(exp)

        elif kind == "W":

            pass
    
        bom     += MonthBegin(-1)
        eom      = bom + MonthEnd(0)
        serial  -= 1

    return res


def get_records_by_contract(
    symbol: str, 
    start:  str, 
    end:    str,
    trim:   bool = True         # trim most contracts without expiried options 
):
    
    filtered = DB.filter(
        (pl.col("name") == symbol)  &
        (pl.col("date") >= start)   & 
        (pl.col("date") < end)
    ).sort(
        [ "date", "year", "month" ]
    )
    
    recs = filtered.select(
        [
            "date",
            "month",
            "year",
            "settle",
            "dte"
        ]
    ).rows()

    cutoff = None

    if trim:

        cutoff = str(datetime.now().year)[2:]

    res = {}

    for rec in recs:

        contract_id = ( rec[base_rec.month],  rec[base_rec.year][2:] )

        if contract_id not in res:

            res[contract_id] = []

        res[contract_id].append(rec)

    if cutoff:

        tmp = {}

        for contract_id, recs in res.items():

            if  contract_id[1] <= cutoff:

                tmp[contract_id] = recs

            res = tmp

    return res