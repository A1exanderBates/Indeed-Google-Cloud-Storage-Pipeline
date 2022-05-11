import base64
import os
import tempfile


from bs4 import BeautifulSoup as bsopa
import requests

import datetime
import csv

from google.cloud import storage

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
with open('/tmp/indeed_scrape1.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['job_title', 'company_name', 'job_location', 'salary'])
    writer.writerows(get_record)


# file path is '/tmp/indeed_scrape1.csv'\
def push_to_gcs(file, bucket):
    file_name = file.split('/')[-1]
    print(f"Pushing {file_name} to GCS...")
    blob = bucket.blob(file_name)
    blob.upload_from_filename(file)
    print(f"File pushed to {blob.id} succesfully.")

file_name = 'indeed_scrape1.csv'
file_path = '/tmp/' + file_name

# Move csv file to Cloud Storage
storage_client = storage.Client()
bucket_name = 'indeed_scrape_bucket'
bucket = storage_client.get_bucket(bucket_name)
push_to_gcs(file_path, bucket)
    
    