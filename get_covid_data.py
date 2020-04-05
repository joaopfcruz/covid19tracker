#!/usr/bin/python3
import time
import requests
import urllib
import logging
import os
import csv
import boto3
import traceback
import codecs
import json
import re
import subprocess
from datetime import date, datetime, timedelta
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from logging.handlers import RotatingFileHandler

#TEMP_CSV = "/home/ec2-user/covid/data.csv" #Downloaded data from source URL
#FINAL_CSV = "/home/ec2-user/covid/data_final.csv" #Final CSV (after data enrichment)
#LOG_FILE = "/home/ec2-user/covid/get_covid_data.log" #Log filename
#TBD NPM
#TBD GIT
#TBD COREUI PROJECTROOT

TEMP_CSV = "data.csv" #Downloaded data from source URL
FINAL_CSV = "data_final.csv" #Final CSV (after data enrichment)
LOG_FILE = "get_covid_data.log" #Log filename
COREUI_PROJECT_ROOT = "/Users/joaopfcruz/Desktop/covid/website"
NPM_BINARY = "/usr/local/bin/npm"
GIT_BINARY = "/usr/bin/git"

#Source: https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide
SRC_URL = "https://opendata.ecdc.europa.eu/covid19/casedistribution/csv/" #Source URL

RETRY_TIMEOUT_SECS = 1800 #Interval between download attempts
MAX_RETRIES = 10 #Number of download retries after exiting

ES_INTERVAL_BETWEEN_CALLS_SECS = 10 # Pause between Elastic calls (performance issues with multiple calls)

#Elastic info
ES_SERVICE = "es"
ES_HOST = "search-coviddatajoaopfcruzdomain-fstgwle3kygnupzqeypagv3h7i.eu-west-3.es.amazonaws.com"
ES_REGION = "eu-west-3"
ES_INDEX = "coviddatajoaopfcruzindex"

#CSV headers (downloaded and final)
EXPECTED_CSV_HEADER = "dateRep,day,month,year,cases,deaths,countriesAndTerritories,geoId,countryterritoryCode,popData2018"
FINAL_CSV_HEADER = "dateRep,day,month,year,cases,deaths,countriesAndTerritories,geoId,countryterritoryCode,popData2018,dayNwithCases,dayNwithDeaths,casesRunningTotal,deathsRunningTotal\n"

#Elastic data mappings
ES_TYPE_MAPPING = {
	"dateRep": {"type":"date", "format":"basic_date"},
	"day": {"type": "keyword"},
	"month": {"type": "keyword"},
	"year": {"type": "keyword"},
	"cases": {"type": "integer"},
	"deaths": {"type": "integer"},
	"countriesAndTerritories": {"type": "keyword"},
	"geoId": {"type": "keyword"},
	"countryterritoryCode": {"type": "keyword"},
	"popData2018": {"type": "integer"},
	"dayNwithCases": {"type": "integer"},
	"dayNwithDeaths": {"type": "integer"},
	"casesRunningTotal": {"type": "integer"},
	"deathsRunningTotal": {"type": "integer"},
	}

#CSV fields
CSV_DELIMITER = "," #Expected CSV delimeter

CSV_DATEREP_FMT = "%d/%m/%Y"
CSV_DATEREP_IDX = 0
CSV_DAY_IDX = 1
CSV_MONTH_IDX = 2
CSV_CASES_IDX = 4
CSV_DEATHS_IDX = 5
CSV_COUNTRY_IDX = 7

FINAL_CSV_DATEREP_IDX = 0
FINAL_CSV_CASES_IDX = 4
FINAL_CSV_DEATHS_IDX = 5
FINAL_CSV_COUNTRY_IDX = 6
FINAL_CSV_GEOID_IDX = 7
FINAL_CSV_POPULATION_IDX = 9
FINAL_CSV_DAYNCASES_IDX = 10
FINAL_CSV_DAYNDEATHS_IDX = 11
FINAL_CSV_CASESRUNNING_IDX = 12
FINAL_CSV_DEATHSRUNNING_IDX = 13


#Thresholds for counting the number of active spreading days
RUNNING_DAYS_CASES_THRESHOLD = 50
RUNNING_DAYS_DEATHS_THRESHOLD = 10

#Stuff for Core UI
COREUI_COUNTRIES_FILE = "/Users/joaopfcruz/Desktop/covid/website/src/data/countries.json"
COREUI_DATA_FILE = "/Users/joaopfcruz/Desktop/covid/website/src/data/data.js"

COREUI_CONST_DATA_DELIM = "#"
COREUI_XAXIS_DATAFMT = "%b %d"
COREUI_MOSTRECENTDATA_DATAFMT = "%B %d, %Y"

COREUI_DATASET_GEOID_IDX = 0
COREUI_DATASET_DATES_IDX = 1
COREUI_DATASET_CASES_IDX = 2
COREUI_DATASET_DEATHS_IDX = 3
COREUI_DATASET_CASESRUNNING_IDX = 4
COREUI_DATASET_DEATHSRUNNING_IDX = 5
COREUI_DATASET_DAYNCASES_IDX = 6
COREUI_DATASET_DAYNDEATHS_IDX = 7
COREUI_DATASET_POPULATION_IDX = 8

GEOID_REGEX = "^[A-Z]{2}$"

try:
	#Logging setup
	logger = logging.getLogger("get_covid_data Logger")
	logger.setLevel(logging.DEBUG)
	handler = RotatingFileHandler(LOG_FILE, maxBytes=10**7, backupCount=5)
	handler.setFormatter(logging.Formatter("%(asctime)s\t[%(levelname)s]\t%(message)s"))
	logger.addHandler(handler)

	#### STEP 1: DOWNLOAD FILE ####
	retries = 0
	retry_download = True
	while retry_download and retries < MAX_RETRIES:
		retries += 1
		logger.info("(DOWNLOAD ATTEMPT %d/%d) Testing URL '%s'" % (retries, MAX_RETRIES, SRC_URL))
		#Test if URL is reachable
		status = requests.get(SRC_URL).status_code
		if status == 200: #URL reachable
			logger.info("(DOWNLOAD ATTEMPT %d/%d) HTTP %d. File found. Downloading..." % (retries, MAX_RETRIES, status))
			urllib.request.urlretrieve(SRC_URL, TEMP_CSV)
			if os.path.exists(TEMP_CSV):
				logger.info("(DOWNLOAD ATTEMPT %d/%d) Downloaded successfully. Saved to '%s'" % (retries, MAX_RETRIES, TEMP_CSV))
				#Check if the file have data for today
				with codecs.open(TEMP_CSV, 'r', encoding='utf-8', errors='ignore') as f:
					data = f.read()
					if date.today().strftime(CSV_DATEREP_FMT) in data:
						retry_download = False
						logger.info("Got updated data!")
					else:
						logger.error("Data not updated yet...")
		else: #URL unreachable
			logger.error("(DOWNLOAD ATTEMPT %d/%d) Failed. HTTP %d" % (retries, MAX_RETRIES, status))

		#Retry the download if needed and possible
		if retry_download:
			if retries < MAX_RETRIES:
				time.sleep(RETRY_TIMEOUT_SECS)
			else:
				logger.fatal("GIVING UP. Exiting.")


	if os.path.exists(TEMP_CSV):
		#Check if the downloaded CSV header is the expected one
		with codecs.open(TEMP_CSV, 'r', encoding='utf-8-sig', errors='ignore') as f:
			csv_header = f.readline()
			header = csv_header.strip()
		if (header == EXPECTED_CSV_HEADER):
			#### STEP 2: Enrich data ####
			## Calculate running days of spreading
			## Calculate running totals for number of cases and deaths
			## Subtracts one day to dateRep for correctness
			logger.info("Enriching data...")
			with codecs.open(TEMP_CSV, 'r', encoding='utf-8-sig', errors='ignore') as f:
				reader = csv.reader(f)
				#Get a list of countries present on data file
				countries = set()
				next(reader)
				for row in reader:
					countries.add(row[CSV_COUNTRY_IDX])

				with open(FINAL_CSV, "w") as ff:
					ff.write(FINAL_CSV_HEADER)
					#For each country, gather all the reported dates.
					#Minimum (Earliest) date will be needed to calculate the number of spreading days
					for c in countries:
						f.seek(0)
						dates_cases_threshold = set()
						dates_deaths_threshold = set()

						sum_cases = 0
						sum_deaths = 0
						for row in reversed(list(reader)):
							if row[CSV_COUNTRY_IDX] == c:
								sum_cases += int(row[CSV_CASES_IDX])
								sum_deaths += int(row[CSV_DEATHS_IDX])

							if row[CSV_COUNTRY_IDX] == c and sum_cases >= RUNNING_DAYS_CASES_THRESHOLD:
								dates_cases_threshold.add(datetime.strptime(row[CSV_DATEREP_IDX], CSV_DATEREP_FMT))
							if row[CSV_COUNTRY_IDX] == c and sum_deaths >= RUNNING_DAYS_DEATHS_THRESHOLD:
								dates_deaths_threshold.add(datetime.strptime(row[CSV_DATEREP_IDX], CSV_DATEREP_FMT))

						f.seek(0)

						sum_cases = 0
						sum_deaths = 0
						for row in reversed(list(reader)):
							if row[CSV_COUNTRY_IDX] == c:
								#Calculate and append the running spreading days (cases based)
								try:
									cases_running_day = (datetime.strptime(row[CSV_DATEREP_IDX], CSV_DATEREP_FMT)-min(dates_cases_threshold)).days
								except ValueError:
									cases_running_day = -1
								row.append(cases_running_day+1 if cases_running_day >= 0 else -1)
								#Calculate and append the running spreading days (deaths based)
								try:
									deaths_running_day = (datetime.strptime(row[CSV_DATEREP_IDX], CSV_DATEREP_FMT)-min(dates_deaths_threshold)).days
								except ValueError:
									deaths_running_day = -1
								row.append(deaths_running_day+1 if deaths_running_day >= 0 else -1)

								#Calculating running totals
								sum_cases += int(row[CSV_CASES_IDX])
								sum_deaths += int(row[CSV_DEATHS_IDX])

								#Write the final CSV (also subtracts one day to dateRep for correctness)
								sub_date = (datetime.strptime(row[CSV_DATEREP_IDX], CSV_DATEREP_FMT) - timedelta(days=1)).strftime(CSV_DATEREP_FMT)
								ff.write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%d\n" % (sub_date, sub_date[:2], sub_date[3:5], sub_date[6:], row[4], row[5], row[6].replace(",", ""), row[7], row[8], row[9], row[10], row[11], sum_cases, sum_deaths))

			logger.info("Data enrichment finished! New CSV saved to %s" % FINAL_CSV)
			logger.info("Building JSON files for webapp")

			#### STEP 3: CONVERT FINAL CSV ROWS TO JSON DATA ####
			logger.info("Converting CSV to JSON documents...")
			bulkdata = ""
			with codecs.open(FINAL_CSV, 'r', encoding='utf-8', errors='ignore') as f:
				splitted_header = FINAL_CSV_HEADER.split(CSV_DELIMITER)
				reader = csv.DictReader(f)
				i = 1
				for row in reader:
					#Add index metadata
					bulkdata += "{\"index\":{\"_index\":\"%s\",\"_type\":\"_doc\",\"_id\":\"%d\"}}\n" % (ES_INDEX, i)
					#Build data document (with a few tweaks)
					json_element = "{"
					for key, value in row.items():
						#Format dateRep to YYYYMMDD (better for data analysis and match ES basic_date format)
						if key == splitted_header[CSV_DATEREP_IDX]:
							value = "%s%s%s" % (value[6:], value[3:5], value[:2])
						#Padding on day and month field (better for data analysis/sorting)
						if key == splitted_header[CSV_DAY_IDX] or key == splitted_header[CSV_MONTH_IDX]:
							value = value.zfill(2)
						json_element += "\"%s\":\"%s\"," % (key, value)
					bulkdata += "%s}\n" % json_element[:-1]
					i += 1
			logger.info("Done converting CSV to JSON documents.")

			#### STEP 4: Build data for webpage (CoreUI) and rebuild page artifacts
			logger.info("Building JSON/Javascript data for webpage.")
			with codecs.open(FINAL_CSV, 'r', encoding='utf-8', errors='ignore') as f:
				f.readline() #skip header

				logger.info("Getting a list of unique countries...")
				#Get a list of countries
				#And a list of dates to get the max date reported
				countries = []
				reported_dates = []
				tmp_delim = "#"
				for row in f:
					split_row = row.split(CSV_DELIMITER)
					if (re.search(GEOID_REGEX, split_row[FINAL_CSV_GEOID_IDX]) and len(split_row[FINAL_CSV_COUNTRY_IDX]) > 0):
						countries.append("%s%s%s" % (split_row[FINAL_CSV_COUNTRY_IDX], tmp_delim, split_row[FINAL_CSV_GEOID_IDX]))
						reported_dates.append(datetime.strptime(split_row[FINAL_CSV_DATEREP_IDX], CSV_DATEREP_FMT))
					else:
						logger.warning("Ignoring data for country '%s' and geoID '%s'" % (split_row[FINAL_CSV_COUNTRY_IDX], split_row[FINAL_CSV_GEOID_IDX]))

				#Remove duplicate countries and format the json object
				final_countries = list(dict.fromkeys(countries))
				final_countries.sort()
				logger.info("Got a list of %d countries." % len(final_countries))
				dict_countries = {}
				logger.info("Building JSON country data...")
				dict_countries["countries"] = []
				for c in final_countries:
					split_row = c.split(tmp_delim)
					#Also replaces underscore with space
					dict_countries["countries"].append({"id":split_row[1], "country":split_row[0].replace("_", " ")})

				logger.info("Writing JSON object to %s..." % COREUI_DATA_FILE)
				#Write countries JSON file
				with open(COREUI_COUNTRIES_FILE, "w") as f:
					f.write(json.dumps(dict_countries))

				if os.path.exists(COREUI_DATA_FILE):
					logger.info("JSON country data successfully written to %s" % COREUI_DATA_FILE)

				logger.info("Building other constant and datasets for webpage...")
				#Write data.js file (multiple constants)
				out = "export const sanitizer_regex=/%s/;\n" % GEOID_REGEX
				out += "export const dataset_delimiter=\"%s\";\n" % COREUI_CONST_DATA_DELIM
				out += "export const dataset_geoid_idx=\"%s\";\n" % COREUI_DATASET_GEOID_IDX
				out += "export const dataset_dates_idx=\"%s\";\n" % COREUI_DATASET_DATES_IDX
				out += "export const dataset_cases_idx=\"%s\";\n" % COREUI_DATASET_CASES_IDX
				out += "export const dataset_deaths_idx=\"%s\";\n" % COREUI_DATASET_DEATHS_IDX
				out += "export const dataset_casesrunning_idx=\"%s\";\n" % COREUI_DATASET_CASESRUNNING_IDX
				out += "export const dataset_deathsrunning_idx=\"%s\";\n" % COREUI_DATASET_DEATHSRUNNING_IDX
				out += "export const dataset_dayncases_idx=\"%s\";\n" % COREUI_DATASET_DAYNCASES_IDX
				out += "export const dataset_dayndeaths_idx=\"%s\";\n" % COREUI_DATASET_DAYNDEATHS_IDX
				out += "export const dataset_population_idx=\"%s\";\n" % COREUI_DATASET_POPULATION_IDX
				out += "export const most_recent_data=\"%s\";\n" % max(reported_dates).strftime(COREUI_MOSTRECENTDATA_DATAFMT)
				out += "export const datasets = ["
				for c in final_countries:
					split_row = c.split(tmp_delim)
					with codecs.open(FINAL_CSV, 'r', encoding='utf-8', errors='ignore') as f:
						dates = ""
						cases = ""
						deaths = ""
						cases_runningtotal = ""
						deaths_runningtotal = ""
						day_n_cases = ""
						day_n_deaths = ""
						population = ""
						for line in f:
							line = line.strip()
							splitted = line.split(CSV_DELIMITER)
							if splitted[FINAL_CSV_GEOID_IDX] == split_row[1]:
								dates += "%s%s" % (datetime.strptime(splitted[FINAL_CSV_DATEREP_IDX], CSV_DATEREP_FMT).strftime(COREUI_XAXIS_DATAFMT), COREUI_CONST_DATA_DELIM)
								cases += "%s%s" % (splitted[FINAL_CSV_CASES_IDX], COREUI_CONST_DATA_DELIM)
								deaths += "%s%s" % (splitted[FINAL_CSV_DEATHS_IDX], COREUI_CONST_DATA_DELIM)
								cases_runningtotal += "%s%s" % (splitted[FINAL_CSV_CASESRUNNING_IDX], COREUI_CONST_DATA_DELIM)
								deaths_runningtotal += "%s%s" % (splitted[FINAL_CSV_DEATHSRUNNING_IDX], COREUI_CONST_DATA_DELIM)
								day_n_cases += "%s%s" % (splitted[FINAL_CSV_DAYNCASES_IDX], COREUI_CONST_DATA_DELIM)
								day_n_deaths += "%s%s" % (splitted[FINAL_CSV_DAYNDEATHS_IDX], COREUI_CONST_DATA_DELIM)
								population = "%s" % splitted[FINAL_CSV_POPULATION_IDX] if len(splitted[FINAL_CSV_POPULATION_IDX]) > 0 else "0"

						out += "[\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"]," % (c.split(tmp_delim)[1], dates[:-1], cases[:-1], deaths[:-1], cases_runningtotal[:-1], deaths_runningtotal[:-1], day_n_cases[:-1], day_n_deaths[:-1], population)

				out = out[:-1]
				out += "];"

				logger.info("Writing constants and datasets to %s" % COREUI_DATA_FILE)
				with open(COREUI_DATA_FILE, "w") as f:
					f.write(out)

				if os.path.exists(COREUI_DATA_FILE):
					logger.info("Constants and datasets successfully written to %s" % COREUI_DATA_FILE)

			if os.path.exists(NPM_BINARY):
				cmd = [NPM_BINARY, "run", "build"]
				logger.info("Running %s..." % cmd)
				proc = subprocess.Popen(cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE, cwd = COREUI_PROJECT_ROOT)
				stdout, stderr = proc.communicate()
				if proc.returncode == 0:
					logger.info("NPM build run successfully. Return code: %d" % proc.returncode)
				else:
					logger.error("***NPM ended with error. Return code: %d" % proc.returncode)
					logger.error("***NPM stdout: %s" % stdout)
					logger.error("***NPM stderr: %s" % stderr)
			else:
				logger.error("NPM binary not found. Will not rebuild page artifacts!")


			#### STEP 5: SEND DATA TO ELASTIC
			#Setup AWS credentials
			credentials = boto3.Session().get_credentials()
			awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, ES_REGION, ES_SERVICE)

			#Setup Elastic connection
			es = Elasticsearch(
				hosts = [{"host": ES_HOST, "port": 443}],
				http_auth = awsauth,
				use_ssl = True,
				verify_certs = True,
				connection_class = RequestsHttpConnection
			)

			logger.info("Updating data on Elastic domain...")
			#Delete index
			logger.info("Deleting index...")
			es.indices.delete(index=ES_INDEX, ignore=[400, 404])
			time.sleep(ES_INTERVAL_BETWEEN_CALLS_SECS)
			#Rebuild index
			logger.info("Recreating index...")
			es.indices.create(index=ES_INDEX)
			time.sleep(ES_INTERVAL_BETWEEN_CALLS_SECS)
			#Rebuild index mapping
			logger.info("Put field mappings...")
			es.indices.put_mapping(
				index=ES_INDEX,
				include_type_name=True,
				doc_type="_doc",
				body={
					"properties": ES_TYPE_MAPPING
				}
			)
			time.sleep(ES_INTERVAL_BETWEEN_CALLS_SECS)
			#Bulk insert data
			logger.info("Inserting data into Elastic")
			es.bulk(bulkdata, request_timeout=30)
			logger.info("Data inserted successfully")

			#Delete CSVs
			logger.info("Deleting %s" % TEMP_CSV)
			os.remove(TEMP_CSV)
			logger.info("Deleting %s" % FINAL_CSV)
			os.remove(FINAL_CSV)
		else:
			logger.fatal("File header differs from expected.")

except Exception as e:
	logger.fatal(traceback.format_exc())

logger.info("Finished. Exiting.")
