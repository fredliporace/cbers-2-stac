# Install packages on lambdas directories

# PATH hack to remove directory in PATH, seems a bug in
# localstack image.
# Thanks https://stackoverflow.com/a/13135333/1259982
echo $PATH
export PATH=$(p=$(echo $PATH | tr ":" "\n" | grep -v "\"" | tr "\n" ":"); echo ${p%:})
echo $PATH

find ./cbers2stac -type f -name 'requirements.txt' -execdir pip install -r {} -t .  \;
