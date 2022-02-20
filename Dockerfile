FROM python:latest
WORKDIR /code
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .
CMD ["wget", "https://drive.google.com/file/d/1kUX10AsnguNpxdjwp26_VrRuEvIU_CBa/view?usp=sharing"]
CMD ["unzip", "./TestData.zip"]
CMD ["python", "./FileStats/Aggregator.py", "--file_list", "./TestData/archive_list.txt", "--processors", "5", "--keywords", "./TestData/100mostcommonwords.txt", "--report", "./TestData/ArchiveStats", "--chunk", "5", "--no-cleanup" ]