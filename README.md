# Abby Sellers Barret Honors Thesis

This repo holds code and data for Abby Sellers thesis. The notebooks hold the
same code as in the python files.

To recreate the dataset you must have python3 installed. Then in your terminal
install the requirements (if you know about virtual environments, do this
inside one otherwise just do this globally):

```
$ pip install -r requirements.txt
```

If you want to use the jupyter notebooks:

```
$ pip install jupyter
```

To get the census data of the median household income in Arizona by zipcode:

```
$ python census_data.py
```

This will create a csv file called ``median_income_by_zip_code.csv``.

To get the school data on the vaccination rates from Arizona Deparatment of
Health Services (this will take a while and take up a lot of your computer's
memory, so get a snack and come back later):

```
$ python az_dhs_vaccines.py
```

This will create multiple csv files with the name format ``Grade_Year.csv``.
