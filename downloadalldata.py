__author__ = 'cnocito'

import urllib.request
import bs4
import os
import gzip
import traceback

def UnzipWriteFile(data,filename):
    try:
        f = open(filename,'wb')
        f.write(bytes(data))
        f.close()
    except:
        print("Unable to download %s" % filename)
        return None
    try:
        f = gzip.open(filename,'rb')
        newFilename = filename.split(".gz")[0]
        outF = open(newFilename,'wb')
        outF.write(f.read())
        f.close()
        outF.close()
        os.remove(filename)
        return 1
    except:
        print("Unable to decompress %s" % filename)
        return None

def DownloadFile(url,filename):
    req = urllib.request.urlopen(url)
    data = req.read()
    UnzipWriteFile(data,filename)

url = "http://api.bitcoincharts.com/v1/csv/"
directory = 'rawfiles'

if not os.path.exists(directory):
    os.makedirs(directory)

req = urllib.request.urlopen(url)
soup = bs4.BeautifulSoup(req.read())
for link in soup.find_all("a"):
    downloadUrl = link['href']
    print("Downloading %s" % downloadUrl)
    try:
        DownloadFile(url+downloadUrl,directory+"/"+downloadUrl)
    except:
        print(traceback.format_exc())