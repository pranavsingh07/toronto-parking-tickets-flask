import os
import pandas as pd
from flask import Flask, Response

app = Flask(__name__)

# Define the base directory for CSV files
CSV_DIR = os.path.abspath(os.path.join(os.path.dirname('./'), "csvs"))

# --------------------------- Helper Functions --------------------------- #

def read_csv_file(csv_file):
    #Helper function to read a CSV file and return its contents.#
    csv_path = os.path.join(CSV_DIR, csv_file)
    
    # Check if file exists
    if not os.path.isfile(csv_path):
        return f"ERROR: File {csv_file} not found on the server", 404
    
    # Read CSV and return as response
    csv_data = pd.read_csv(csv_path)
    headers = {'Content-Type': 'text/csv'}
    return Response(csv_data.to_csv(index=False), headers=headers)

# --------------------------- API Endpoints --------------------------- #

@app.route('/')
def index():
    #Home route - simple greeting message.#
    return 'Welcome to the Parking Ticket Data API!'

@app.route('/agg/address')
def agg_address():
    #Returns aggregated parking ticket data by address.#
    return read_csv_file("agg/agg_address.csv")

@app.route('/agg/code')
def agg_code():
    #Returns aggregated parking ticket data by violation code.#
    return read_csv_file("agg/agg_code.csv")

@app.route('/agg/code/<id>')
def agg_code_by_id(id):
    #Returns specific violation code details.#
    csv_file = "agg/agg_code.csv"
    csv_path = os.path.join(CSV_DIR, csv_file)

    if not os.path.isfile(csv_path):
        return f"ERROR: File {csv_file} not found on the server", 404
    
    csv_data = pd.read_csv(csv_path)
    csv_data = csv_data[csv_data['Code'] == int(id)]  # Filter by violation code
    if csv_data.empty:
        return f"ERROR: No records found for violation code {id}", 404
    headers = {'Content-Type': 'text/csv'}
    
    return Response(csv_data.to_csv(index=False), headers=headers)

@app.route('/agg/prov')
def agg_prov():
    #Returns aggregated ticket data by province of vehicle plate.#
    return read_csv_file("agg/agg_prov.csv")
  
@app.route('/agg/provcoord')
def agg_prov_w_coord():
    #Returns aggregated ticket data by province of vehicle plate.#
    return read_csv_file("agg/agg_prov_w_coord.csv")

@app.route('/agg/fine_per_day')
def agg_fine_per_day():
    #Returns aggregated fines collected per day.#
    return read_csv_file("agg/agg_fine_per_day.csv")

@app.route('/agg/dayofweek')
def agg_dayofweek():
    #Returns aggregated ticket data by day of the week.#
    return read_csv_file("agg/agg_dayofweek.csv")

@app.route('/agg/month')
def agg_month():
    #Returns aggregated ticket data by month.#
    return read_csv_file("agg/agg_month.csv")

@app.route('/agg/hour')
def agg_hour():
    #Returns aggregated ticket data by hour of the day.#
    return read_csv_file("agg/agg_hour.csv")

@app.route('/dayofweek/<day>')
def dayofweek(day):
    #Returns ticket data for a specific day of the week.#
    dir_path = os.path.join(CSV_DIR, "dayofweek", day)

    if not os.path.exists(dir_path) or not os.listdir(dir_path):
        return f"ERROR: No data found for {day}", 404

    csv_paths = [os.path.join(dir_path, f) for f in os.listdir(dir_path) if f.endswith('.csv')]
    csv_data = pd.concat([pd.read_csv(f) for f in csv_paths], ignore_index=True)
    
    headers = {'Content-Type': 'text/csv'}
    return Response(csv_data.sort_values(by="issue_date").to_csv(index=False), headers=headers)

@app.route('/total')
def total():
    #Returns the full dataset of parking tickets.#
    dir_path = os.path.join(CSV_DIR, "total")

    if not os.path.exists(dir_path) or not os.listdir(dir_path):
        return "ERROR: No data found in the total directory", 404

    csv_paths = [os.path.join(dir_path, f) for f in os.listdir(dir_path) if f.endswith('.csv')]
    csv_data = pd.concat([pd.read_csv(f) for f in csv_paths], ignore_index=True)
    
    headers = {'Content-Type': 'text/csv'}
    return Response(csv_data.sort_values(by=["Issue_Date", "Issue_Time"]).to_csv(index=False), headers=headers)

@app.route('/month/<month>')
def month(month):
    #Returns ticket data for a specific month.#
    dir_path = os.path.join(CSV_DIR, "month", month)

    if not os.path.exists(dir_path) or not os.listdir(dir_path):
        return f"ERROR: No data found for {month}", 404

    csv_paths = [os.path.join(dir_path, f) for f in os.listdir(dir_path) if f.endswith('.csv')]
    csv_data = pd.concat([pd.read_csv(f) for f in csv_paths], ignore_index=True)
    
    headers = {'Content-Type': 'text/csv'}
    return Response(csv_data.sort_values(by="Issue_Date").to_csv(index=False), headers=headers)

# Run the Flask app
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=81)
