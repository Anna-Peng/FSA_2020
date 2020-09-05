import json
import os
import subprocess
import shutil
import glob
from FOOBAR import prependpath
from FOOBAR import FOOBAR

def test_fbmktplace_json():
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

 #   if os.path.exists(project_path):
 #       shutil.rmtree(project_path)

    start_command = ["scrapy", "startproject", project_name]
    print(' '.join(start_command))
    os.system(' '.join(start_command))

    inputfiles = glob.glob(source_path + '/*Spider.py')
    for infile in inputfiles:
        copy_command = ['cp', infile, copy_path]
        if os.name == 'nt': 
            copy_command.insert(0, 'powershell')
        print(' '.join(copy_command))
        os.system(' '.join(copy_command))
        #subprocess.run(copy_command, check=True)
    #run the spider on test mode
    command = ['scrapy','crawl','fbmktplace','-o','fbmktplace_test.json','--set=ROBOTSTXT_OBEY="False"','--set=DOWNLOAD_DELAY=1.0','-a','city=london','-a','keywords="homemade food"','-a','max_n_results=200','-a','scroll_pause_time=2.0','-a','mode=test']
    os.chdir(prependpath(project_name))
    os.system(' '.join(command))
    with open('fbmktplace_test.json') as f:
        test_json = json.load(f)
        assert isinstance(test_json[0], dict)
        assert list(test_json[0].keys()) == ['heading','description','location','search_term1','search_term2','time']
    os.remove('fbmktplace_test.json')
    os.system(cwd)


