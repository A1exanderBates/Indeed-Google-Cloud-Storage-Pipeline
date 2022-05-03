import base64
import os
import tempfile
from werkzeug.utils import secure_filename

import requests
from bs4 import BeautifulSoup as bsopa
import datetime
import csv

from google.cloud import storage

bucket_name = os.environ['indeed_scrape_bucket'] #without gs://
file_name = os.environ['indeed_scrape1.csv']
cf_path = '/tmp/{}'.format(file_name)

# file path is '/tmp/indeed_scrape1.csv'
def scrape_pubsub(event, context):

    # set storage client
    client = storage.Client()

    # get bucket
    bucket = client.get_bucket(bucket_name)

    # download the file to Cloud functions's tmp directory
    
    def get_file_path(filename):
        file_name = secure_filename(filename)
        return os.path.join(tempfile.gettempdir(), file_name)

    def write_temp_dir():
        # Scraping the data
        get_record = []

        position = 'Data Engineer'
        location = ['Seattle, WA', 'San Francisco, CA'] 

        for k in location:    
            for i in range(0,10,1): # calling 10 entries   
                y=requests.get('https://www.indeed.com/jobs?q={}&l={}&sort=date='.format(position, k)+str(i))

                sou=bsopa(y.text,'lxml')

                for j in sou.find_all('div',{"class":"job_seen_beacon"}):
                    i=j.find('tbody') # calling the table body to go inside of
                    a= i.find('tr')

                    # Job Title
                    for n in a.find_all('h2',{'class':'jobTitle jobTitle-color-purple jobTitle-newJob'}):

                        job_title = n.find_all('span')[1].get_text()# if you don't use the 1, you get the 'new' posting text
                        

                        # company name
                        a.find('div',{'class':'heading6 company_location tapItem-gutter'})
                        company_name = a.find('span',{'class':'companyName'}).get_text()
                        print(a.find('span',{'class':'companyName'}).get_text())

                        # location
                        a.find('div',{'class':'heading6 company_location tapItem-gutter'})
                        job_location = a.find('div',{'class':'companyLocation'}).get_text()
                        print(a.find('div',{'class':'companyLocation'}).get_text())

                        # salary if available
                        try:
                            if a.find('div',{'class':'heading6 tapItem-gutter metadataContainer noJEMChips salaryOnly'}):
                                salary = a.find('div',{'class':'metadata salary-snippet-container'}).get_text()
                            else:
                                salary = a.find('div',{'class':'attribute_snippet'}).get_text()
                        except:
                                salary = ''
                        get_record.append([job_title, company_name, job_location, salary])
                get_record

        # creating a csv file
        with open('indeed_scrape1.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['job_title', 'company_name', 'job_location', 'salary'])
            writer.writerows(get_record)
        
        #data = 'indeed_scrape1.csv'
        #path_name = get_file_path(data)


        # set Blob
        blob = storage.Blob('indeed_scrape1.csv', bucket)

        # if above doesn't work, try:
        # blob = storage.Blob(data, bucket)

        # upload the file to GCS
        blob.upload_from_filename(cf_path)

        # delete temp directory
        os.remove(cf_path)

    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)
    
