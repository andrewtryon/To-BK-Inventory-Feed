from dotenv import load_dotenv
load_dotenv()
import os
import time
import ftplib
import pandas as pd
import pyodbc
import requests
from datetime import date
import json

def makeWrikeTask (title = "New Pricing Task", description = "No Description Provided", status = "Active", assignees = "KUAAY4PZ", folderid = "IEAAJKV3I4JBAOZD"):
    url = "https://www.wrike.com/api/v4/folders/" + folderid + "/tasks"
    querystring = {
        'title':title,
        'description':description,
        'status':status,
        'responsibles':assignees
        } 
    headers = {
        'Authorization': 'bearer TOKEN'.replace('TOKEN',os.environ.get(r"WRIKE_TOKEN"))
        }        
    response = requests.request("POST", url, headers=headers, params=querystring)
    return response

def attachWrikeTask (attachmentpath, taskid):
    url = "https://www.wrike.com/api/v4/tasks/" + taskid + "/attachments"
    headers = {
        'Authorization': 'bearer TOKEN'.replace('TOKEN',os.environ.get(r"WRIKE_TOKEN"))
    }

    files = {
        'X-File-Name': (attachmentpath, open(attachmentpath, 'rb')),
    }

    response = requests.post(url, headers=headers, files=files)
    return response       


if __name__ == '__main__':

    try:

        #1
        sage_conn_str = os.environ.get(r"sage_conn_str").replace("UID=;","UID=" + os.environ.get(r"sage_login") + ";").replace("PWD=;","PWD=" + os.environ.get(r"sage_pw") + ";")      

        #Establish sage connection
        sage_cnxn = pyodbc.connect(sage_conn_str, autocommit=True)
        #SQL Sage data into dataframe
        SageSQLquery = """
            SELECT CI_Item.ItemCode, IM_ItemWarehouse.QuantityOnHand, IM_ItemWarehouse.QuantityOnSalesOrder, IM_ItemVendor.VendorAliasItemNo
            FROM CI_Item, IM_ItemWarehouse, IM_ItemVendor 
            WHERE (
                (CI_Item.PrimaryVendorNo = 'BKPR001') AND 
                (CI_Item.InactiveItem <> 'Y') AND
                (CI_Item.PrimaryVendorNo = IM_ItemVendor.VendorNo) AND
                (CI_Item.ItemCode = IM_ItemVendor.ItemCode) AND
                (CI_Item.ItemCode = IM_ItemWarehouse.ItemCode) AND
                (IM_ItemWarehouse.WarehouseCode = '000')
                )
        """
        print('Retrieving Sage data')
        sageDF = pd.read_sql(SageSQLquery,sage_cnxn)

        #2
        sageDF['QuantityAvailable'] = sageDF['QuantityOnHand'] - sageDF['QuantityOnSalesOrder']
        sageDF = sageDF.query("QuantityAvailable > 0")

        #3
        filename = '14524_import_' + date.today().strftime('%Y-%m-%d') + '.csv'
        print(filename)
        #exit()
        sageDF.to_csv(r'\\FOT00WEB\Alt Team\Kris\GitHubRepos\Vendor Stock Feeds\BK' + '\\' + filename, header=True, index=False, columns = ['VendorAliasItemNo','QuantityAvailable'])

        time.sleep(15)

        #4 added sleepys to appear less bot like
        ftp = ftplib.FTP()
        host = os.environ.get(r"BK_HOST_IP")
        port = int(os.environ.get(r"BK_PORT"))
        ftp.connect(host, port)
        time.sleep(3)
        print (ftp.getwelcome())
        time.sleep(3)
        print ("Logging in...")
        print(ftp.login(os.environ.get(r"BK_FEED_LOGIN"), os.environ.get(r"BK_FEED_PW")))
        print ("Logged in...")
        time.sleep(3)
        file = open(r'\\FOT00WEB\Alt Team\Kris\GitHubRepos\Vendor Stock Feeds\BK' + '\\' + filename,'rb')        
        print('ftping file')
        ftp.storbinary('STOR /' + filename, file)
        time.sleep(30)
        print('ftp complete')
        file.close()
        print('dun-ski')
        ftp.quit()          
    except:
        print("rut-ro")
        assignees = '[KUACOUUA,KUAEL7RV,KUAAY4PZ,KUALCDZR]'#,KUALCDZR,KUAEL7RV]' # Andrew, Anthony
        folderid = 'IEAAJKV3I4JEW3BI' #Web Requests IEAAJKV3I4GOVKOA
        wrikedescription = "BK Precision Feed Error - Alert Andrew! Script can be found here -> Kris\GitHubRepos\Vendor Stock Feeds\BK \r Last generated file attached"
        wriketitle = date.today().strftime('%Y-%m-%d')+ " - Morningly BK Precision Feed Error"
        response = makeWrikeTask(title = wriketitle, description = wrikedescription, assignees = assignees, folderid = folderid)
        response_dict = json.loads(response.text)
        taskid = response_dict['data'][0]['id']
        filetoattachpath = r'\\FOT00WEB\Alt Team\Kris\GitHubRepos\Vendor Stock Feeds\BK' + '\\' + filename
        print('Attaching file')
        attachWrikeTask(attachmentpath = filetoattachpath, taskid = taskid)         
        print('File attached!')   
    finally: 
        pass

