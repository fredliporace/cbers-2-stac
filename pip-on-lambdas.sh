# Install packages on lambdas directories
find ./cbers2stac -type f -name 'requirements.txt' -execdir pip install -r {} -t .  \;
