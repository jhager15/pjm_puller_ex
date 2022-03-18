import pandas as pd
import os.path
from pathlib import Path

from info.file_formats import output_path, acct_lens, auto_814, out_814, out_867, auto_867
from adder.model import adder
from preprocess.process import sub_preprocess, date_list, file_getter, preprocess_814, preprocess_867, final_867, \
    final_acct_status, generate_filename, group_filter, output_filename, send_email
from info.global_vars import QTR_DTE, SUB_PATH


def long_int_pull(sheet_name, lookback):
    print('_________%s_________' % (sheet_name))

    #create ouput dir if it doesn't already exist
    Path("%s\%s" % (output_path[sheet_name], QTR_DTE)).mkdir(parents=True, exist_ok=True)

    sub_data, sub_date_max, df_group_list = sub_preprocess(SUB_PATH, sheet_name, acct_lens[sheet_name], lookback)

    # create list of file dates to be pulled
    dlist = date_list(sub_date_max, lookback)

    # get 814 data
    frame_814, len_814 = file_getter(dlist, auto_814[sheet_name], out_814[sheet_name], 'EDI_814_Response_Output','UtilAcctNo')

    # get 867 data
    frame_867, len_867 = file_getter(dlist, auto_867[sheet_name], out_867[sheet_name], '_867HU_All_from_EdiFile','AccountNumber')

    # prepend 'E' to all 814 data to keep leading zero accts intact
    frame_814 = preprocess_814(frame_814,acct_lens[sheet_name])
    frame_867 = preprocess_867(frame_867, sheet_name,acct_lens[sheet_name])

    # merge sub data with 867 data
    grouped_867 = final_867(frame_867, sub_data)
    # Merge grouping with back acct data to see if accounts are missing
    status_df = final_acct_status(sub_data, frame_867, frame_814)

    # usage adding
    cal_summary, tab_summary, ts_data = adder(grouped_867, sheet_name)

    # merge with sub sheet
    if len_867 == 0:
        print('No 867 data for %s' % (sheet_name))
    else:
        # remove dupes key = ACCT# & FROM & TO & USE .... + data cleaning
        # Apply grouplist items to 867 data and the same to the 814 data
        print('_________RFP STATUS_________')
        for n in df_group_list:

            # generate all variable needed to create xlsx file
            filtered_867, filtered_status_df, filtered_cal_summary, filtered_tab_summary, filtered_ts_data = group_filter(grouped_867, status_df, cal_summary, tab_summary, ts_data, n)
            not_back_len = len(filtered_status_df[filtered_status_df["Acct Status"] == 'Not Back'])
            complete_filename, partial_filename = generate_filename(n, output_path[sheet_name], not_back_len)

            # check if the accounts needed for this rfp are in the 867 data, if not, it means they're not back from the edi
            if int(len(filtered_867)) == 0: print('Data not back from EDI:  [%s] ' % (n))

            # check if this rfp has already been pulled for this qtr
            elif os.path.isfile(complete_filename) or os.path.isfile(partial_filename) == True:
                print('Already exists:  [%s] ' % (n))

            # compiles and sends rfp data that made if through the past 2 elif statements
            else:
                dfs = {
                    'Acct_Data': filtered_status_df,
                    '867 Data': filtered_867,
                    'cal_summary': filtered_cal_summary,
                    'tab_summary': filtered_tab_summary.dropna(axis=1, how='all'),
                    'ts_data': filtered_ts_data
                }

                # generates name of excel file
                name = output_filename(complete_filename, partial_filename, not_back_len)

                writer = pd.ExcelWriter(name, engine='xlsxwriter')  # var
                for sheet in dfs.keys():
                    dfs[sheet].to_excel(writer, sheet_name=sheet, index=False)
                writer.save()
                send_email('pricing@aggressiveny.com',
                           'jhager@aggressiveny.com',
                           'xxxxxxx',
                           '%s--%s' % (sheet_name, n),
                           n,
                           name
                           )
#long_int_pull('JCPL',10)