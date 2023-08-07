from    datetime                import  datetime, timedelta
from    dateutil.relativedelta  import  FR, relativedelta
from    enum                    import  IntEnum
from    json                    import  loads
from    pandas                  import  date_range, DateOffset, Timestamp
from    pandas.tseries.offsets  import  BDay, BusinessMonthEnd, LastWeekOfMonth, WeekOfMonth
import  polars                  as      pl
from    sys                     import  argv
from    time                    import  time
from    typing                  import  List


CONFIG      = loads(open("./config.json").read())
DATE_FMT    = "%Y-%m-%d"
DB          = pl.read_parquet(CONFIG["db_path"])
EXPIRATIONS = {
    "NG": { 
        "M": ( -1, "EXP", 1 )
    },
    "CL": {
        "M": ( -3, "EXP", 1 ),
        "W": ( "MWF", 1 )
    },
    "ZN": {
        "M": ( -1, "FRI", 3 ),
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

    res     = []
    dates   = [ rec[base_rec.date] for rec in recs ]
    dte     = [ rec[base_rec.dte] for rec in recs ]
    ul_exp  = Timestamp(dates[0]) + DateOffset(days = dte[0])

    if kind == "M":

        offset      = rule[0]
        relative_to = rule[1]
        months      = rule[2]

        if relative_to == "EOM":

            # business days prior to end of month

            pass

        elif relative_to == "EXP":

            # business days prior to underlying expiration

            while months > 0:

                res.append((ul_exp - offset * BDay()).strftime(DATE_FMT))

                months -= 1

        elif relative_to == "FRI":

            # friday of month

            exp = ul_exp + relativedelta(day = 31, weekday = FR(-1))

            while months > 0:

                while not BDay().onOffset(exp):

                    exp -= BDay()

                res.append(exp.strftime(DATE_FMT))
            
                exp += DateOffset(months = 1)
                exp += relativedelta(day = 31, weekday = FR(-1))

                months  -= 1

    elif kind == "W":

        days    = [ DAYS_OF_WEEK[day] for day in rule[0] ]
        months  = rule[1]
        exp     = ul_exp
        month   = ul_exp.month

        while months > 0:

            if exp.weekday() in days:

                res.append(exp)

                exp -= BDay()

                if exp.month != month:

                    month = ul_exp.month

                    months -= 1

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