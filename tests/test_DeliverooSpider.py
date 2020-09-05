import json
import os
import subprocess
import shutil
import glob
from FOOBAR import prependpath
from FOOBAR import FOOBAR

def test_deliveroo_json():
#from FOOBAR.py scrape method
    cwd = os.getcwd()
    parameters = prependpath('parameters.yml')
    foobar = FOOBAR(parameters)
    project_name = foobar._parameters['scrapy']['project']
    project_path = prependpath(foobar._parameters['scrapy']['project'])
    source_path = prependpath(foobar._parameters['scrapy']['source'])
    copy_path = prependpath(foobar._parameters['scrapy']['copy'])
    data_path = prependpath(foobar._parameters['scrapy']['data'])
    spiders = foobar._parameters['scrapy']['spiders']

    try:
        spiders.values()
    except AttributeError as error:
        print("FOOBAR::scrape", error)
        return

#    if os.path.exists(project_path):
#        shutil.rmtree(project_path)

    start_command = ["scrapy", "startproject", project_name]
    print(' '.join(start_command))
    os.system(' '.join(start_command))
    #subprocess.run(start_command, check=True)

    inputfiles = glob.glob(source_path + '/*Spider.py')
    for infile in inputfiles:
        copy_command = ['cp', infile, copy_path]
        if os.name == 'nt': 
            copy_command.insert(0, 'powershell')
        print(' '.join(copy_command))
        os.system(' '.join(copy_command))
        #subprocess.run(copy_command, check=True)
    #run the spider on test mode
    command = ['scrapy','crawl','deliveroo','-o','deliveroo_test.json','--set=ROBOTSTXT_OBEY="False"','--set=DOWNLOAD_DELAY=1.0','-a','postcode=E16AW','-a','mode=test']
    os.chdir(prependpath(project_name))
    print(' '.join(command))
    os.system(' '.join(command))
    with open('deliveroo_test.json') as f:
        test_json = json.load(f)
        assert isinstance(test_json[0], dict)
        assert list(test_json[0].keys()) == ['heading','rating','description','fsa_id','fhrs','extras','search_term1','time']
    os.remove('deliveroo_test.json')
    os.system(cwd)


