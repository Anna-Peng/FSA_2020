import subprocess
import shutil
import glob
import os
import yaml
from pkg_resources import Requirement, resource_filename
from src.features.SqliteCreator import SqliteCreator

##################################################################################################
# FOOBAR class
##################################################################################################

class FOOBAR:

    def __init__(self, paramfile='parameters_alt.yml'):
        """
        Creates FHRS Online Platforms object

        Arguments
        ---------
        paramfile: string with path to parameter file
        """
        with open(paramfile, 'r') as stream:
            try:
                parameters = yaml.safe_load(stream)
            except yaml.YAMLError as error:
                print("FOOBAR::__init__", error)
        self._parameters = parameters

    def scrape(self):
        """
        Creates *.json files from scraping
        """
        project_name = self._parameters['scrapy']['project']
        project_path = prependpath(self._parameters['scrapy']['project'])
        source_path = prependpath(self._parameters['scrapy']['source'])
        copy_path = prependpath(self._parameters['scrapy']['copy'])
        data_path = prependpath(self._parameters['scrapy']['data'])
        spiders = self._parameters['scrapy']['spiders']

        try:
            spiders.values()
        except AttributeError as error:
            print("FOOBAR::scrape", error)
            return

        if os.path.exists(project_path):
            shutil.rmtree(project_path)

        start_command = ["scrapy", "startproject", project_name]
        print(' '.join(start_command))
        subprocess.run(start_command, check=True)

        inputfiles = glob.glob(source_path + '/*Spider.py')
        for infile in inputfiles:
            copy_command = ['cp', infile, copy_path]
            if os.name == 'nt': 
                copy_command.insert(0, 'powershell')
            print(' '.join(copy_command))
            subprocess.run(copy_command, check=True)

        #HB: replace this with scrapy API call https://docs.scrapy.org/en/0.16/topics/practices.html
        for spider in spiders:
            print(f"\nScraping {spider}")
            command = ['scrapy', 'crawl', spider, '--set=ROBOTSTXT_OBEY="False"', '--set=DOWNLOAD_DELAY=1.0']
            outfile = spider
            for key, value in spiders[spider].items():
                command.append("-a")
                command.append(key+'='+'"'+value+'"')
                outfile += "_"+value.replace(" ", "")
            outfile += ".json"
            command.append("-o")
            command.append(outfile)
            print(' '.join(command))
            os.chdir(prependpath(project_name))
            os.system(' '.join(command))

        inputfiles = glob.glob(prependpath(project_name) + "/*.json")
        for infile in inputfiles:
            move_command = ["mv", infile, data_path]
            if os.name == 'nt': 
                move_command.insert(0, 'powershell')
            print(' '.join(move_command))
            subprocess.run(move_command, check=True)

    def preprocess(self):
        """
        Creates *.csv files from scraped *.json files
        """
        platforms = self._parameters['platforms']

        for platform in platforms:
            preprocessor = platforms[platform]['preprocessor']
            prefix = prependpath(platforms[platform]['prefix'])
            outdir = prependpath(platforms[platform]['outdir'])
            inputfiles = glob.glob(prefix + "*")

            if not os.path.exists(outdir):
                os.mkdir(outdir)

            for infile in inputfiles:
                filename = os.path.splitext(os.path.basename(infile))[0] + ".csv"
                outfile = os.path.join(outdir, filename)
                preprocess_jsonfile(infile, preprocessor, outfile)

    def prepare(self):
        """
        Creates *_dbpart.csv files from user-editable csv files
        """
        generated_platforms = prependpath(self._parameters['generated']['platforms'])
        generated_platformplatformtypes = prependpath(self._parameters['generated']['platformplatformtypes'])
        generated_platformtypes = prependpath(self._parameters['generated']['platformtypes'])
        generated_businesstypes = prependpath(self._parameters['generated']['businesstypes'])
        editable_platforms = prependpath(self._parameters['editable']['platforms'])
        editable_platformtypes = prependpath(self._parameters['editable']['platformtypes'])
        script_platforms = prependpath('src/features/preparePlatforms.py')
        script_businesstypes = prependpath('src/features/prepareBusinessTypes.py')

        print(f'''
            \nPreparing {prettyprint(generated_platforms)}, {prettyprint(generated_platformplatformtypes)}, {prettyprint(generated_platformtypes)} 
from {prettyprint(editable_platforms)}, {prettyprint(editable_platformtypes)}
            ''')
        subprocess.run(["python", script_platforms, "-i", editable_platforms, "-j", editable_platformtypes, "-k", generated_platforms, "-l", generated_platformplatformtypes, "-m", generated_platformtypes], check=True)

        print(f"\nPreparing {prettyprint(generated_businesstypes)} with {prettyprint(script_businesstypes)}")
        subprocess.run(["python", script_businesstypes, "-o", generated_businesstypes], check=True)

    def sqlize(self):
        platforms = self._parameters['platforms']
        generated_platforms = prependpath(self._parameters['generated']['platforms'])
        generated_platformplatformtypes = prependpath(self._parameters['generated']['platformplatformtypes'])
        generated_platformtypes = prependpath(self._parameters['generated']['platformtypes'])
        generated_businesstypes = prependpath(self._parameters['generated']['businesstypes'])
        database = prependpath(self._parameters['database'])

        foobar_database = SqliteCreator(prependpath(self._parameters['database']))
        foobar_database.create()
        foobar_database.populatePlatforms(generated_platforms, generated_platformtypes, generated_platformplatformtypes, generated_businesstypes)

        for platform in platforms:
            print(f"\nSQLizing {platform}\n")
            prefix = prependpath(platforms[platform]['prefix'])
            outdir = prependpath(platforms[platform]['outdir'])
            inputfiles = glob.glob(prefix + "*")

            for infile in inputfiles:
                filename = os.path.splitext(os.path.basename(infile))[0] + ".csv"
                outfile = os.path.join(outdir, filename)
                foobar_database.insertListings(outfile)

        print(f"Completed database file: {prettyprint(database)}")

        foobar_database.commit()
        foobar_database.close()

##################################################################################################
# utility functions for front-end
##################################################################################################

def prependpath(filename):
    """
    Returns the path that prepends the repository's top directory to the filename
    """
    return resource_filename(Requirement.parse('foobar'), filename)

def prettyprint(filename):
    """
    Returns the basename+suffix of the filename
    """
    return os.path.splitext(os.path.basename(filename))[0] + os.path.splitext(os.path.basename(filename))[1]

def preprocess_jsonfile(infile, preprocessor, outfile):
    """
    Creates *.csv files using the preprocessor member function of a Preprocessor subclass
    """
    module = __import__(f"src.features.{preprocessor}", fromlist=[preprocessor])
    PlatformPreprocessor = getattr(module, preprocessor)

    print(f"\nPreprocessing {prettyprint(infile)} with {preprocessor}")
    preprocInstance = PlatformPreprocessor(infile)
    dat_processed = preprocInstance.get_dataframe()

    print(f"Writing to file: {prettyprint(outfile)}")
    dat_processed.to_csv(outfile, index=False)

##################################################################################################
def main():

    parameters = prependpath('parameters.yml')
    foobar = FOOBAR(parameters)

    foobar.scrape()
    foobar.preprocess()
    foobar.prepare()
    foobar.sqlize()

if __name__ == "__main__":
    main()
