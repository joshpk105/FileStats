1. Run docker-compose up
    - This will generate the benchmark data (run_profile.sh) and create an interactive container
    - statistics.txt is found at ./TestData/statistics.txt
    - Processor.py profile data is found at ./TestData/*.cProfile
2. We can read *.cProfile data by starting an interactive python session and using pstats
    - https://docs.python.org/3/library/profile.html