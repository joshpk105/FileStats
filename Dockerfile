FROM python:latest
WORKDIR /code
COPY . .
RUN pip install -r requirements.txt
RUN gdown https://drive.google.com/uc?id=1kUX10AsnguNpxdjwp26_VrRuEvIU_CBa
RUN unzip ./TestData.zip
RUN python -m cProfile -o AggregatorProfile.cProfile ./FileStats/Aggregator.py --file_list ./TestData/archive_list.txt --processors 5 --keywords ./TestData/100mostcommonwords.txt --report ./TestData/ArchiveStats --chunk 5 --no-cleanup --profile
