import glob
from io import BytesIO
import zipfile
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
import requests, zipfile, os, shutil

def readZipFile():
    url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/datasource.zip'
    try:
        fileLoc = os.getcwd()+'/data'
        os.mkdir(fileLoc)
    except:
        shutil.rmtree(fileLoc)
        os.mkdir(fileLoc)
    req = requests.get(url)
    # Split URL to get the file name
    filename = url.split('/')[-1]
    zip_file = zipfile.ZipFile(BytesIO(req.content))
    zip_file.extractall(fileLoc)
    # Writing the file to the local file system
    with open(filename,'wb') as output_file:
        output_file.write(req.content)

def extract_from_csv(file_to_process):
    df = pd.read_csv(file_to_process)
    return df

def extract_from_json(file_to_process):
    df = pd.read_json(file_to_process,lines=True)
    return df

def extract_from_xml(file_to_process):
    df = pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for car in root:
        car_model = car.find('car_model').text
        year_manu = car.find('year_of_manufacture').text
        price = float(car.find('price').text)
        fuel = car.find('fuel').text
        df2 = pd.DataFrame({'car_model':[car_model], 'year_of_manufacture':[year_manu], 'price':[price], 'fuel':[fuel] })
        #df2 = pd.DataFrame([car_model,year_manu,price,fuel],columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])
        df = pd.concat([df,df2])
    return df

def extract():
    readZipFile()
    extracted_data = pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])
    #extract CSVs
    for csvfile in glob.glob("/data/*.csv"):
        #extracted_data = extracted_data.append(extract_from_csv(csvfile), ignore_index= True)
        extracted_data = pd.concat([extracted_data, extract_from_csv(csvfile)])

    #extract JSON
    for jsonfile in glob.glob("/data/*.json"):
        extracted_data = pd.concat([extracted_data,extract_from_json(jsonfile)])

    #extract xml
    for xmlfile in glob.glob("/data/*.xml"):
        extracted_data = pd.concat([extracted_data,extract_from_xml(xmlfile)])

    return extracted_data

def transform(data):
    data['price'] = round(data.price,2)
    return data

def load(targetFile, dataToLoad):
    dataToLoad.to_csv(targetFile)

def log(message):
    timestampFormat = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()
    timestamp = now.strftime(timestampFormat)
    with open('logfile.txt','a') as f:
        f.write(timestamp + ',' + message + '\n')

#main function

targetfile = "transformed_data.csv"   # file where transformed data is stored
log('ETL Job Started')
log('Extract Phase Started')
extractedData = extract()
log('Extract Phase Ended')
log('Transform Phase Started')
transformedData = transform(extractedData)
log('Transform Phase Ended')
transformedData
log('Load Phase Started')
load(targetfile,transformedData)
log('Load Phase Ended')
log('ETL Job Ended')
log('\n'+'*********************************************************' + '\n')

