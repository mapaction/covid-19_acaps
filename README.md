# covid-19_acaps

## Getting started

* Create a conda environment using Python 3.8 and activate it:
```buildoutcfg
conda create --name covid-19_acaps python=3.8
activate covid-19_acaps
```
* Install the requirements:
```buildoutcfg
conda install --file requirements.txt
```
`hdx-python-api` is also required but not available through conda. Install it using pip:
```buildoutcfg
pip install hdx-python-api==4.3.5
```

## Running
The one required runtime parameter is the name of the crash move folder. 
On Windows this will likely be `Z:\charlie`.
Execute the script as follows:
```buildoutcfg
python main.py Z:\charlie
```
If you would like to run the script without querying the API every time
by using the cached ACAPS data file, add the `--debug` flag:
 ```buildoutcfg
python main.py Z:\charlie --debug
```
