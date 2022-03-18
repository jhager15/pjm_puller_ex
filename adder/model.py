import pandas as pd
from pandas.tseries.offsets import MonthEnd
import numpy as np
from calendar import monthrange
from datetime import timedelta

from info.file_formats import read_type,month_days


def eomonth(date):
    return pd.to_datetime(date, format="%Y%m") + MonthEnd(0)

def prev_eomonth(date):
    return (date + pd.offsets.MonthEnd(0) - pd.offsets.MonthBegin(normalize=True))-timedelta(days=1)

def attr_days(start,end,attr):
    return ((np.maximum((attr-start).dt.days,0) - np.maximum((prev_eomonth(attr) - start).dt.days,0))-(np.maximum((attr-end).dt.days,0) - np.maximum((prev_eomonth(attr) - end).dt.days,0))+((eomonth(attr)==eomonth(start))*1))

def days_in_month(year, month):
    return monthrange(year, month)[1]

def summary(x):
    result = {
        'RFP_Name'      : x['RFP_Name'].min(),
        'prorated_use'  : x['prorated_use'].sum(),
        'attr_days_sum' : x['attr_days'].sum(),
    }
    return pd.Series(result)


def _my_date_range(x,from_date,to_date):
    return list(
        pd.date_range(start =x[from_date],
                      end   =x[to_date] + pd.offsets.MonthEnd(),
                      freq  ='M').strftime('%Y-%m-%d')
        )


def adder(df_mtrs, utility):
    #prorate & index enermark data
    df_mtrs = df_mtrs[['RFP_Name', 'AccountNumber', 'FromDate', 'ToDate', 'ProjectedUsage']].copy()

    df_mtrs.FromDate = pd.to_datetime(df_mtrs.FromDate,errors='coerce')
    df_mtrs.ToDate = pd.to_datetime(df_mtrs.ToDate,errors='coerce')

    df_mtrs = df_mtrs.dropna(subset=['FromDate'])
    df_mtrs = df_mtrs.dropna(subset=['ToDate'])

    df_mtrs['ToDate_adj'] = df_mtrs['ToDate'] - pd.to_timedelta(read_type[utility], unit='d')
    df_mtrs['mtr_days'] = ((df_mtrs['ToDate_adj'] - df_mtrs['FromDate']).dt.days) + 1
    df_mtrs['attr'] = df_mtrs['FromDate']
    df_mtrs['attr_days'] = df_mtrs['attr']
    df_mtrs['daily_use'] = df_mtrs['ProjectedUsage'] / df_mtrs['mtr_days']

    #data['attr'] = data.apply(_my_date_range, args=(from_date, to_date), axis=1)

    df_mtrs = pd.concat([pd.DataFrame({
                            'attr': pd.date_range(row.attr, row.ToDate + pd.offsets.MonthEnd(), freq='M'),
                            'RFP_Name': row.RFP_Name,
                            'AccountNumber': row.AccountNumber,
                            'FromDate': row.FromDate,
                            'ToDate': row.ToDate,
                            'ToDate_adj': row.ToDate_adj,
                            'mtr_days': row.mtr_days,
                            'daily_use': row.daily_use,
                                       }, columns=['attr', 'RFP_Name','AccountNumber','FromDate','ToDate','ToDate_adj','mtr_days','daily_use'])
                         for i, row in df_mtrs.iterrows()], ignore_index=True)

    df_mtrs['attr_days'] = attr_days(df_mtrs['FromDate'], df_mtrs['ToDate_adj'], df_mtrs['attr'])
    df_mtrs['attr_month'] = df_mtrs['attr'].dt.month
    df_mtrs = df_mtrs[df_mtrs['attr_days'] > 0].reset_index(drop=True)
    df_mtrs['prorated_use'] = df_mtrs['attr_days'] * df_mtrs['daily_use']

    cal_summary = _cal_summary(df_mtrs)

    # gives prorated usage summary, from first meter to last, tabular
    ts_data, tab_summary = _tab_summary(df_mtrs)

    #time series summary
    ts_data = _ts_summary(ts_data)

    return cal_summary, tab_summary, ts_data


def _cal_summary(df_mtrs):
    # 12 month wt. avg prorated summary; df unstacked
    cal_summary = df_mtrs.groupby(['AccountNumber', 'attr_month']).apply(summary).reset_index()
    cal_summary['daily'] = cal_summary['prorated_use'] / cal_summary['attr_days_sum']
    cal_summary['days_month'] = cal_summary['attr_month'].map(month_days)
    cal_summary['use'] = cal_summary['days_month'] * cal_summary['daily']
    cal_summary = cal_summary.pivot_table(index=['RFP_Name','AccountNumber'], columns='attr_month', values='use').reset_index()

    # move rfp name to first col
    cal_summary = cal_summary[['RFP_Name'] + [col for col in cal_summary.columns if col != 'RFP_Name']]

    return cal_summary


def _ts_summary(ts_data):
    
    ts_data['month_days'] = ts_data['attr'].dt.days_in_month
    ts_data = ts_data[['RFP_Name','AccountNumber','attr','attr_days_sum','month_days','prorated_use']]
    ts_data = ts_data.loc[(ts_data['attr_days_sum'] == ts_data['month_days'])].reset_index(drop=True)

    # move rfp name to first col
    ts_data = ts_data[['RFP_Name'] + [col for col in ts_data.columns     if col != 'RFP_Name']]

    return ts_data


def _tab_summary(df_mtrs):

    tab_summary = df_mtrs.groupby(["AccountNumber", "attr"]).apply(summary).reset_index()
    ts_data = tab_summary[['RFP_Name','AccountNumber','attr','attr_days_sum','prorated_use']]
    tab_summary = tab_summary.pivot(index=['RFP_Name','AccountNumber'], columns='attr', values='prorated_use').reset_index()

    # move rfp name to first col
    tab_summary = tab_summary[['RFP_Name'] + [col for col in tab_summary.columns if col != 'RFP_Name']]

    return ts_data, tab_summary


def final_ts_data(ts_data):
    def summary2(x):
        result = {
            'prorated_use': x['prorated_use'].sum(),
            'attr_count': x['attr'].count(),
            'acct_count': x['AccountNumber'].count(),
        }
        return pd.Series(result)

    ts_data = ts_data.groupby(['RFP_Name','attr']).apply(summary2).reset_index()
    return ts_data