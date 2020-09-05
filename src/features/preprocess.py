import sys, getopt
import importlib

##########################################################################################
# When run in command line
##########################################################################################

def main(argv):
    in_json = ''
    in_preprocessorClass = ''
    out_csv = ''
    try:
        opts, args = getopt.getopt(argv,"hi:p::o:",["in=","preprocessorclass=", "out="])
    except getopt.GetoptError:
        print('preprocess.py -i <[platform]_[postcode]_[date].json> -p "DeliverooPreprocessor" -o <[platform]_[postcode]_[date].csv>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('preprocess.py -i <[platform]_[postcode]_[date].json> -p "DeliverooPreprocessor" -o <[platform]_[postcode]_[date].csv>')
            sys.exit()
        elif opt in ("-i", "--in"):
            in_json = arg
        elif opt in ("-p", "--preprocessorclass"):
            in_preprocessorClass = arg
        elif opt in ("-o", "--out"):
            out_csv = arg
    
    # Preprocess the provided file with the provided preprocessor
    if all(len(f) > 0 for f in [in_json, in_preprocessorClass, out_csv]):
        # import the relevant preprocessor module
        module = __import__('src.features.%s' %in_preprocessorClass, fromlist=[in_preprocessorClass])# importlib.import_module('src.features.%s.%s' % (in_preprocessorClass, in_preprocessorClass))
        PlatformPreprocessor = getattr(module, in_preprocessorClass)
        # preprocess
        print('Preprocessing "%s" with "%s"' % (in_json, in_preprocessorClass))
        preprocInstance = PlatformPreprocessor(in_json)

        # write to file
        print("Writing to file: %s" % out_csv)
        dat_processed = preprocInstance.get_dataframe()
        print(dat_processed)
        dat_processed.to_csv(out_csv, index = False)
    else:
        print("Please provide all the required parameters")
        print('usage: preprocess.py -i <[platform]_[postcode]_[date].json> -p "DeliverooPreprocessor" -o <[platform]_[postcode]_[date].csv>')
        sys.exit()

if __name__ == "__main__":
    main(sys.argv[1:])
