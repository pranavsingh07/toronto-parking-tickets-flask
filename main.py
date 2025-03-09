import os
import pandas as pd
from flask import Flask, send_file, Response
from pandas.core.common import is_null_slice
from pandas.core.dtypes.missing import isna

app = Flask(__name__)


@app.route('/')
def index():
  return 'Hello from Flask!'


@app.route('/agg/address')
def agg_address():
  csv_dir = "./csvs/agg/"
  csv_file = "agg_address.csv"
  csv_path = os.path.join(csv_dir, csv_file)

  # Also make sure the requested csv file does exist
  if not os.path.isfile(csv_path):
    return "ERROR: file %s was not found on the server" % csv_file
  # Send the file back to the client
  csv_data = pd.read_csv(csv_path)
  headers = {'Content-type': 'text/plain'}
  response = Response(csv_data.to_csv(index=False), headers=headers)
  # response.headers["Content-Disposition"] = "attachment; filename=users.csv"

  return response


@app.route('/agg/code')
def agg_code():
  csv_dir = "./csvs/agg/"
  csv_file = "agg_code.csv"
  csv_path = os.path.join(csv_dir, csv_file)

  # Also make sure the requested csv file does exist
  if not os.path.isfile(csv_path):
    return "ERROR: file %s was not found on the server" % csv_file
  # Send the file back to the client
  csv_data = pd.read_csv(csv_path)
  headers = {'Content-type': 'text/plain'}
  response = Response(csv_data.to_csv(index=False), headers=headers)

  return response


@app.route('/agg/code/<id>')
def agg_code_by_id(id):
  csv_dir = "./csvs/agg/"
  csv_file = "agg_code.csv"
  csv_path = os.path.join(csv_dir, csv_file)

  # Also make sure the requested csv file does exist
  if not os.path.isfile(csv_path):
    return "ERROR: file %s was not found on the server" % csv_file
  # Send the file back to the client
  csv_data = pd.read_csv(csv_path)
  headers = {'Content-type': 'text/plain'}
  print(id)
  csv_data = csv_data[csv_data['code'] == int(id)]
  response = Response(csv_data.to_csv(index=False), headers=headers)

  return response


@app.route('/agg/prov')
def agg_prov():
  csv_dir = "./csvs/agg/"
  csv_file = "agg_prov.csv"
  csv_path = os.path.join(csv_dir, csv_file)

  # Also make sure the requested csv file does exist
  if not os.path.isfile(csv_path):
    return "ERROR: file %s was not found on the server" % csv_file
  # Send the file back to the client
  csv_data = pd.read_csv(csv_path)
  headers = {'Content-type': 'text/plain'}
  response = Response(csv_data.to_csv(index=False), headers=headers)

  return response


@app.route('/agg/fine_per_day')
def agg_fine_per_day():
  csv_dir = "./csvs/agg/"
  csv_file = "agg_fine_per_day.csv"
  csv_path = os.path.join(csv_dir, csv_file)

  # Also make sure the requested csv file does exist
  if not os.path.isfile(csv_path):
    return "ERROR: file %s was not found on the server" % csv_file
  # Send the file back to the client
  csv_data = pd.read_csv(csv_path)
  headers = {'Content-type': 'text/plain'}
  response = Response(csv_data.to_csv(index=False), headers=headers)

  return response


@app.route('/agg/dayofweek')
def agg_dayofweek():
  csv_dir = "./csvs/agg/"
  csv_file = "agg_dayofweek.csv"
  csv_path = os.path.join(csv_dir, csv_file)

  # Also make sure the requested csv file does exist
  if not os.path.isfile(csv_path):
    return "ERROR: file %s was not found on the server" % csv_file
  # Send the file back to the client
  csv_data = pd.read_csv(csv_path)
  headers = {'Content-type': 'text/plain'}
  response = Response(csv_data.to_csv(index=False), headers=headers)

  return response


@app.route('/dayofweek/<day>')
def dayofweek(day):
  csv_dir = "./csvs/dayofweek/" + day
  csv_file = "*.csv"
  csv_paths = []
  if not os.listdir(csv_dir):
    return "ERROR: path %s was empty" % day
  for f in os.listdir(csv_dir):
    csv_paths.append(os.path.join(csv_dir, f))

  # Also make sure the requested csv file does exist
  for csv_path in csv_paths:
    if not os.path.isfile(csv_path):
      return "ERROR: file %s was not found on the server" % csv_file

  # Send the file back to the client

  csv_data = pd.concat([pd.read_csv(f) for f in csv_paths], ignore_index=True)
  csv_data.name = day
  headers = {'Content-type': 'text/plain'}
  response = Response(
      csv_data.sort_values(by="issue_date").to_csv(index=False),
      headers=headers)

  return response


@app.route('/total')
def total():
  csv_dir = "./csvs/total/"
  csv_file = "*.csv"
  csv_paths = []
  if not os.listdir(csv_dir):
    return "ERROR: path %s was empty"
  for f in os.listdir(csv_dir):
    csv_paths.append(os.path.join(csv_dir, f))

  # Also make sure the requested csv file does exist
  for csv_path in csv_paths:
    if not os.path.isfile(csv_path):
      return "ERROR: file %s was not found on the server" % csv_file

  # Send the file back to the client

  csv_data = pd.concat([pd.read_csv(f) for f in csv_paths], ignore_index=True)
  headers = {'Content-type': 'text/plain'}
  response = Response(
      csv_data.sort_values(by=["Issue_Date", 'Issue_Time']).to_csv(index=False),
      headers=headers)

  return response


app.run(host='0.0.0.0', port=81)
