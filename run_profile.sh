# Script used to collect benchmarking data
# We can read *.cProfile files using python module pstats
# See: https://docs.python.org/3/library/profile.html
gdown https://drive.google.com/uc?id=1kUX10AsnguNpxdjwp26_VrRuEvIU_CBa
unzip ./TestData.zip
python -m cProfile -o Aggregator.cProfile ./FileStats/Aggregator.py --file_list ./TestData/archive_list.txt --processors 5 --keywords ./TestData/100mostcommonwords.txt --report ./TestData/ArchiveStats --chunk 5 --no-cleanup --profile
