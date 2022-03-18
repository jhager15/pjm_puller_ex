import pandas as pd
pd.set_option('display.expand_frame_repr', False)
import glob
from datetime import date, timedelta
from collections import OrderedDict
import os.path

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

import re

from info.file_formats import LDC_codes
from info.global_vars import QTR_DTE


def file_finder(dlist,auto_path,out_path):
    def dir_1(dlist):
        return glob.glob(
            auto_path % (dlist[0], dlist[1], dlist[2], dlist[3], dlist[4], dlist[5], dlist[6], dlist[7]))
    def dir_2(dlist):
        return glob.glob(
            out_path % (dlist[0], dlist[1], dlist[2], dlist[3], dlist[4], dlist[5], dlist[6], dlist[7]))

    final_867_key = flatten_list(list(map(dir_1, dlist))) + flatten_list(list(map(dir_2, dlist)))
    file_list = list(OrderedDict.fromkeys(final_867_key))
    files_len = len(file_list)
    return file_list,files_len

def sub_preprocess(path,sheet_name,utility_len,lookback):
    sub_data = pd.read_excel(path, index_col=None, header=0, sheet_name=sheet_name,
                             converters={'AccountNumber': lambda x: str(x)})

    sub_data = sub_data[['AccountNumber', 'Day Submitted', 'RFP Name', 'Broker']]
    sub_data = sub_data.dropna(subset=['AccountNumber']).reset_index()
    sub_data = sub_data.rename(columns={'RFP Name': 'RFP_Name'})
    sub_data['RFP_Name'] = sub_data['RFP_Name'].str.replace('[#,@,\,/,?,:,;]', ' ', regex=True)
    sub_data[['AccountNumber', 'RFP_Name', 'Broker']] = sub_data[['AccountNumber', 'RFP_Name', 'Broker']].apply(lambda x: x.str.strip())
    sub_data['AccountNumber'] = sub_data['AccountNumber'].str.strip()
    sub_data['AccountNumber'] = sub_data['AccountNumber'].str.zfill(utility_len)
    sub_data['AccountNumber'] = sub_data['AccountNumber'].apply("E{}".format)
    sub_data[['Day Submitted', 'RFP_Name', 'Broker']] = sub_data[['Day Submitted', 'RFP_Name', 'Broker']].ffill()
    sub_data['Day Submitted'] = pd.to_datetime(sub_data['Day Submitted'], errors='coerce').ffill()
    #sub_data['Day Submitted'] = np.where(sub_data['Day Submitted'] > date.today(),date.today(),sub_data['Day Submitted'])

    sub_date_max = sub_data['Day Submitted'].max()
    sub_date_min = sub_date_max - timedelta(days=lookback)

    sub_data = sub_data[sub_data['Day Submitted'] >= sub_date_min]
    sub_data = sub_data[['AccountNumber', 'RFP_Name', 'Broker']]

    df_group_list = sub_data['RFP_Name'].unique()

    df_group_list = [x for x in df_group_list if str(x) != 'nan']


    print('_________ENTITIES TO PULL_________')
    for n in df_group_list:
        print(n)

    return sub_data,sub_date_max,df_group_list


def cap_preprocess(cap_data_path):
    list_of_files = glob.glob(cap_data_path)
    latest_file = max(list_of_files, key=os.path.getctime)

    cap_data = pd.read_csv(latest_file, index_col=0, header=0,
                           converters={'AccountNumber': lambda x: str(x)})

    cap_data['Date'] = pd.to_datetime(cap_data['Date']).dt.strftime("%Y%m%d").apply(str)
    return cap_data


def file_getter(dlist, auto_path, out_path,tab_name,acct_title):
    try:
        file_list,files_len = file_finder(dlist, auto_path, out_path)
        li = []
        for filename in file_list:
            df = pd.read_excel(filename, index_col=None, header=0, sheet_name=tab_name,
                               converters={acct_title: lambda x: str(x)})
            li.append(df)
        frame = pd.concat(li, axis=0, ignore_index=True)
        return frame,files_len

    except ValueError as e:
        if int(re.search(r'\d+', tab_name).group(0))==867:
            print('867 error:')
            print('Directory path:[%s]'%auto_path)
            print('has no data for dates: %s' % (dlist.tolist()))
            print('python error: [%s]'%e)
        else:
            print('814 error:')
            print('Directory path:[%s]'%auto_path)
            print('has no data for dates: %s' % (dlist.tolist()))
            print('python error: [%s]'%e)
        frame = []
        files_len = []
        return frame,files_len


def preprocess_867(frame_867,sheet_name,utility_len):
    if 'TotalUsage' in frame_867.columns: frame_867 = frame_867.rename(columns={'TotalUsage':'ProjectedUsage'})
    if 'LDC' not in frame_867.columns:
        frame_867 = frame_867.rename(columns={'AcctNo':'LDC'})
        frame_867['LDC'] = str(LDC_codes[sheet_name])

    frame_867['dupe_key'] = frame_867['AccountNumber'].astype(str) + " " + \
                            frame_867['FromDate'].astype(str) + " " + \
                            frame_867['ToDate'].astype(str) + " " + \
                            frame_867['ProjectedUsage'].astype(str)
    frame_867 = frame_867.drop_duplicates(subset=['dupe_key']).drop(columns=['dupe_key'])
    frame_867['AccountNumber'] = frame_867['AccountNumber'].str.zfill(utility_len)
    frame_867['AccountNumber'] = frame_867['AccountNumber'].apply("E{}".format)
    return frame_867


def preprocess_814(frame_814,utility_len):
    try:
        frame_814['UtilAcctNo'] = frame_814['UtilAcctNo'].str.zfill(utility_len)
        frame_814['UtilAcctNo'] = frame_814['UtilAcctNo'].apply("E{}".format)
        return frame_814
    except:
        return frame_814


def date_list(sub_date_max,lookback):
    sub_from_date = sub_date_max - timedelta(days=lookback)
    sub_to_date = date.today() + timedelta(days=1)
    dlist = pd.date_range(sub_from_date, sub_to_date, freq='d')

    #show list in terminal
    print('_________DATES TO PULL_________')
    for n in dlist:
        print(n)
    return dlist.strftime("%Y%m%d")


def final_867(frame_867,sub_data):
    grouped_867 = pd.merge(frame_867, sub_data, on="AccountNumber", how="inner")
    grouped_867 = grouped_867.dropna(subset=["RFP_Name"]).reset_index(drop=True)
    return grouped_867


def final_acct_status(sub_data,frame_867,frame_814):
    sub_data = pd.merge(sub_data[['AccountNumber', 'RFP_Name']], frame_867[['AccountNumber', 'LDC']],
                           on='AccountNumber', how='left')
    sub_data = sub_data.copy(deep=True)
    sub_data['LDC'] = sub_data['LDC'].fillna('Not Back')


    sub_data.columns = ['AccountNumber', 'RFP_Name', 'Acct Status']
    sub_data['Acct Status'].loc[(sub_data['Acct Status'] != 'Not Back')] = 'Back'

    sub_data = sub_data.drop_duplicates(subset=['AccountNumber'], keep='first').reset_index(drop=True)

    try:
        status_df = pd.merge(sub_data, frame_814[['UtilAcctNo', 'Description', 'Reason']],
                              left_on='AccountNumber', right_on='UtilAcctNo', how='left')

        status_df = status_df.drop_duplicates(subset=['AccountNumber']).reset_index(drop=True)
        status_df = status_df.drop(columns='UtilAcctNo')
        return status_df
    except TypeError:
        status_df = sub_data.drop_duplicates(subset=['AccountNumber']).reset_index(drop=True)
        return status_df



def generate_filename(n,output_path,not_back_len):
    complete_filename = r'%s\%s\%s.xlsx' % (output_path,QTR_DTE,n)
    partial_filename = r'%s\%s\%s - (%s).xlsx' % (output_path, QTR_DTE, n,str(not_back_len))
    return complete_filename,partial_filename


def output_filename(complete_filename,partial_filename,not_back_len):
    return partial_filename if not_back_len > 0 else complete_filename


def flatten_list(_2d_list):
    flat_list = []
    for element in _2d_list:
        if type(element) is list:
            for item in element:
                flat_list.append(item)
        else:
            flat_list.append(element)
    return flat_list


def send_email(email_recipient, email_sender, email_passwrd, email_subject, email_message, attachment_location=''):
    msg = MIMEMultipart()
    msg['From'] = email_sender
    msg['To'] = email_recipient
    msg['Subject'] = email_subject

    msg.attach(MIMEText(email_message, 'plain'))

    if attachment_location != '':
        filename = os.path.basename(attachment_location)
        attachment = open(attachment_location, "rb")
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition',
                        "attachment; filename= %s" % filename)
        msg.attach(part)

    try:
        server = smtplib.SMTP('smtp.office365.com', 587)
        server.ehlo()
        server.starttls()
        server.login(email_sender,email_passwrd)
        text = msg.as_string()
        server.sendmail(email_sender, email_recipient, text)
        print('email sent: [%s]'%(email_subject))
        server.quit()
    except:
        print("SMPT server connection error: %s"%(email_subject))
    return True


def group_filter(grouped_867,status_df,cal_summary,tab_summary,ts_data,n):
    filtered_867 = grouped_867[grouped_867['RFP_Name'] == n]
    filtered_status_df = status_df[status_df['RFP_Name'] == n]

    filtered_cal_summary = cal_summary[cal_summary['RFP_Name'] == n]
    filtered_tab_summary = tab_summary[tab_summary['RFP_Name'] == n]
    filtered_ts_data = ts_data[ts_data['RFP_Name'] == n]

    return filtered_867, filtered_status_df, filtered_cal_summary, filtered_tab_summary, filtered_ts_data
