# Signals Service

___
1. Build docker image 

```
docker build -t signals:latest .
```

2. Run docker container.
 - Note that you can insert absolute path to the directory instead of $PWD
 - Use INPUT_PATH variable to specify the input file
 - Use OUTPUT_PATH variable to specify the output file.
Or by default will be used the filename from INPUT_PATH

Version 1 - with OUTPUT_PATH:
```
docker run \
    -v $PWD:/code/ \
    -e INPUT_PATH=signals \
    -e OUTPUT_PATH=signals_result \
    signals:latest
```

Version 2 - without OUTPUT_PATH:
```
docker run \
    -v $PWD:/code/ \
    -e INPUT_PATH=signals \
    signals:latest
```

3. Check output results in OUTPUT_PATH dir.
___
