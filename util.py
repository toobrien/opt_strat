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
OPT_DEFS    = [

    # ul sym, opt sym, freq, desc, rule, serial months, day of week, enabled

    ( "NG",     "LNE",      "M",    "European Options",             "EOM-4BD",      1,  None,   True    ),  # also UL_EXP-1BD
    ( "NG",     "ON",       "M",    "American Options",             "EOM-4BD",      1,  None,   False   ),  # also UL_EXP-1BD
    ( "CL",     "LO",       "M",    "American Options",             "EOM-4BD",      1,  None,   True    ),
    ( "CL",     "LO1:5",    "W",    "Weekly Friday",                None,           1,  4,      True    ),
    ( "CL",     "WL1:5",    "W",    "Weekly Wednesday",             None,           1,  2,      True    ),
    ( "CL",     "ML1:5",    "W",    "Weekly Monday",                None,           1,  0,      True    ),
    ( "HO",     "OH",       "M",    "American Options",             "EOM-4BD",      1,  None,   True    ),
    ( "RB",     "OB",       "M",    "American Options",             "EOM-4BD",      1,  None,   True    ),
    ( "ZN",     "OZN",      "M",    "American Options",             "EOM-2BD-1FRI", 3,  None,   True    ),
    ( "ZN",     "ZN1:5",    "W",    "Weekly Options",               None,           3,  4,      True    ),
    ( "ZN",     "WY1:5",    "W",    "Weekly Wednesday Option",      None,           3,  2,      True    ),
    ( "ZB",     "OZB",      "M",    "American Options",             "EOM-2BD-1FRI", 3,  None,   True    ),
    ( "ZB",     "ZB1:5",    "W",    "Weekly Options",               None,           3,  4,      True    ),
    ( "ZB",     "WB1:5",    "W",    "Weekly Wednesday Option",      None,           3,  2,      True    ),
    ( "ZC",     "OZC",      "M",    "American Options",             "EOM-2BD-1FRI", 2,  None,   True    ),
    ( "ZC",     "OCD",      "M",    "Short-Dated New Crop Options", "EOM-2BD-1FRI", 12, None,   True    ),  # Z only... need to implement
    ( "ZC",     "ZC1:5",    "W",    "Weekly Options",               None,           2,  4,      True    ),
    ( "ZW",     "OZW",      "M",    "American Options",             "EOM-2BD-1FRI", 3,  None,   True    ),
    ( "ZW",     "ZW1:5",    "W",    "Weekly Options",               None,           3,  4,      True    ),
    ( "ZS",     "OZS",      "M",    "American Options",             "EOM-2BD-1FRI", 3,  None,   True    ),
    ( "ZS",     "OSD",      "M",    "Short-Dated New Crop Options", "EOM-2BD-1FRI", 12, None,   True    ),  # X only... need to implement
    ( "ZS",     "ZS1:5",    "W",    "Weekly Options",               None,           3,  4,      True    ),
    ( "ZM",     "OZM",      "M",    "American Options",             "EOM-2BD-1FRI", 1,  None,   True    ),  # not sure about serial
    ( "ZL",     "OZL",      "M",    "American Options",             "EOM-2BD-1FRI", 1,  None,   True    ),  # not sure about serial
    ( "HE",     "HE",       "M",    "American Options",             "BOM+10BD",     1,  None,   True    ),  # not sure about serial
    ( "LE",     "LE",       "M",    "American Options",             "BOM+1FRI",     1,  None,   True    ),  # not sure about serial
    ( "GF",     "GF",       "M",    "American Options",             "EOM-(1|2)THU", 1,  None,   True    ),  # not sure about serial
    ( "GC",     "OG",       "M",    "American Options",             "EOM-(4|5)BD",  1,  None,   True    ),  # not sure about serial
    ( "GC",     "OG1:5",    "W",    "Weekly Options",               None,           2,  4,      True    ),  # not sure about serial
    ( "GC",     "G1:5W",    "W",    "Weekly Wednesday Option",      None,           2,  2,      True    ),  # not sure about serial
    ( "GC",     "G1:5M",    "W",    "Weekly Monday Option",         None,           2,  0,      True    ),  # not sure about serial
    ( "SI",     "SO",       "M",    "American Options",             "EOM-(4|5)BD",  3,  None,   True    ),  # not sure about serial
    ( "SI",     "SO1:5",    "W",    "Weekly Options",               None,           3,  4,      True    ),  # not sure about serial
    ( "SI",     "W1:5S",    "W",    "Weekly Wednesday Option",      None,           3,  2,      True    ),  # not sure about serial
    ( "SI",     "M1:5S",    "W",    "Weekly Monday Option",         None,           3,  0,      True    ),  # not sure about serial
    ( "HG",     "HXE",      "M",    "American Options",             "EOM-(4|5)BD",  3,  None,   True    ),  # not sure about serial
    ( "HG",     "H1:5E",    "W",    "Weekly Options",               None,           3,  4,      True    ),  # not sure about serial
    ( "HG",     "H1:5W",    "W",    "Weekly Wednesday Option",      None,           3,  2,      True    ),  # not sure about serial
    ( "HG",     "H1:5M",    "W",    "Weekly Monday Option",         None,           3,  0,      True    ),  # not sure about serial
    ( "6E",     "EUU",      "M",    "Monthly Options",             "BOM+2FRI<3WED", 3,  None,   True    ),
    ( "6E",     "1:5EU",    "W",    "Weekly Friday Options",        None,           3,  4,      True    ),
    ( "6E",     "SU1:5",    "W",    "Weekly Thursday Option",       None,           3,  3,      True    ),
    ( "6E",     "WE1:5",    "W",    "Weekly Wednesday Options",     None,           3,  2,      True    ),
    ( "6E",     "TU1:5",    "W",    "Weekly Tuesday Option",        None,           3,  1,      True    ),
    ( "6E",     "MO1:5",    "W",    "Weekly Monday Option",         None,           3,  0,      True    ),
    ( "6J",     "JPU",      "M",    "Monthly Options",             "BOM+2FRI<3WED", 3,  None,   True    ),
    ( "6J",     "1:5JY",    "W",    "Weekly Friday Options",        None,           3,  4,      True    ),
    ( "6J",     "SJ1:5",    "W",    "Weekly Thursday Option",       None,           3,  3,      True    ),
    ( "6J",     "WJ1:5",    "W",    "Weekly Wednesday Options",     None,           3,  2,      True    ),
    ( "6J",     "TJ1:5",    "W",    "Weekly Tuesday Option",        None,           3,  1,      True    ),
    ( "6J",     "MJ1:5",    "W",    "Weekly Monday Option",         None,           3,  0,      True    )
    
]
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
    0: "MON",
    1: "TUE",
    2: "WED",
    3: "THU",
    4: "FRI"
}
HOLIDAYS = USFederalHolidayCalendar.holidays(start = "1900-01-01", end = "2100-01-01")


class opt_def_rec(IntEnum):

    ul_sym  = 0
    opt_sym = 1
    freq    = 2
    desc    = 3
    rule    = 4
    serial  = 5
    day     = 6
    enabled = 7


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
    dfn:    opt_def_rec
):

    res         = []
    dates       = [ rec[base_rec.date] for rec in recs ]
    dte         = [ rec[base_rec.dte] for rec in recs ]
    ul_exp      = Timestamp(dates[0]) + DateOffset(days = dte[0])
    bom         = ul_exp + MonthBegin(-1)
    eom         = ul_exp + MonthEnd(0)
    rule        = dfn[opt_def_rec.rule]
    r_name      = rule[0] 
    serial      = rule[1]

    while serial > 0:

        if kind == "M":

            if r_name == "UL_EXP-1BD":

                # one business day prior to the underlying contract's expiration

                res.append(ul_exp - BDay())

            elif r_name == "EOM-4BD":
                
                # fourth last business day of the month

                exp = bdate_range(bom, eom, freq = "C", holidays = HOLIDAYS)[-4] 

                res.append(exp)
            
            elif r_name == "EOM-2BD-1FRI":

                # first friday prior to the second to last business day of the month;
                # if this is not a business day, then the day prior

                cutoff  = bdate_range(bom, eom, holidays = HOLIDAYS)[-3]
                fri     = date_range(bom, cutoff, freq = "W-FRI")[-1]
                exp     = fri if BDay().is_on_offset(fri) else fri - BDay()

                res.append(exp)

            elif r_name == "25TH-(6|7)BD":

                # the 6th day prior to the 25th calendar day of the month, if business day; else, the 7th day prior

                exp = bom + DateOffset(days = 24) - 6 * BDay()
                exp = exp if BDay().is_on_offset(exp) else exp - BDay()

            elif r_name == "BOM+10BD":

                # tenth business day of the month

                exp = bdate_range(bom, eom, freq = "C", holidays = HOLIDAYS)[10]

                res.append(exp)

            elif r_name == "BOM+1FRI":

                # first friday of month

                exp = date_range(bom, eom, freq = "W-FRI")[1]

                res.append(exp)

            elif r_name == "EOM-(1|2)THU":

                # last thursday of month if business day; else 2nd last thursday

                rng = date_range(bom, eom, freq = "W-THU")
                exp = rng[-1] if BDay().is_on_offset(rng[-1]) else rng[-2]

                res.append(exp)

            elif r_name == "EOM-(4|5)BD":

                # 4th last business day of the month, unless friday (or holiday); else 5th last business day of month

                exp = bdate_range(bom, eom, freq = "C", holidays = HOLIDAYS)[-4]
                exp = exp if exp.day_of_week == 4 else exp - BDay()

                res.append(exp)

            elif r_name == "BOM+2FRI<3WED":

                # 2nd friday of the month prior to the 3rd wednesday of the month

                third_wed   = date_range(bom, eom, freq = "W-WED")[2]
                exp         = date_range(bom, third_wed, freq = "W-FRI")[-2]

                res.append(exp)

        elif kind == "W":

            day = dfn[opt_def_rec.day]
            rng = date_range(bom, ul_exp, freq = f"W-{DAYS_OF_WEEK[day]}")
            
    
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