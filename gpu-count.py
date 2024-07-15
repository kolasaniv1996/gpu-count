import requests
import json
import argparse
from collections import defaultdict
import pandas as pd
import os
from datetime import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots



# Configuration
APPNAME = 'APPNAME'
APPSECRET = 'APPSECRET'
APPURL = 'https://xxxx.com'
REALM = 'runai'



def login():
    payload = f"grant_type=client_credentials&client_id={APPNAME}&client_secret={APPSECRET}"
    headers = {'content-type': 'application/x-www-form-urlencoded'}
    url = f"{APPURL}/auth/realms/{REALM}/protocol/openid-connect/token"
    r = requests.post(url, headers=headers, data=payload)
    if r.status_code // 100 == 2:
        return json.loads(r.text)['access_token']
    else:
        print("Login error: " + r.text)
        exit(1)

def get_job_information(token, cluster_id):
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    params = {
        'filter': 'jobType:Train|Interactive',
        'sortBy': 'status',
        'sortDirection': 'asc',
        'from': 0,
        'limit': 20
    }

    url = f"{APPURL}/v1/k8s/clusters/{cluster_id}/jobs/"
    try:
        r = requests.get(url, headers=headers, params=params)
        r.raise_for_status()  # Raises an HTTPError for bad responses
        print("200")  # Print 200 for successful API call
        return r.json()
    except requests.exceptions.RequestException as e:
        print("Script is not working")
        return None

def consolidate_jobs(jobs):
    user_jobs = defaultdict(lambda: {'totalRequestedGPUs': 0.0, 'jobCount': 0})
    for job in jobs:
        user = job.get('user', 'Unknown')
        gpus = job.get('totalRequestedGPUs', '0')  # Get as string, default to '0'
        try:
            gpus = float(gpus)  # Convert to float to handle fractional GPUs
        except ValueError:
            print(f"Warning: Invalid GPU value '{gpus}' for user {user}. Setting to 0.")
            gpus = 0.0
        user_jobs[user]['totalRequestedGPUs'] += gpus
        user_jobs[user]['jobCount'] += 1

    return [{'user': user, 'totalRequestedGPUs': round(data['totalRequestedGPUs'], 2), 'jobCount': data['jobCount']}
            for user, data in user_jobs.items()]

def save_to_csv(data, cluster_id):
    audit_folder = 'audit'
    if not os.path.exists(audit_folder):
        os.makedirs(audit_folder)

    df = pd.DataFrame(data)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"job_audit_{cluster_id}_{timestamp}.csv"
    filepath = os.path.join(audit_folder, filename)

    df.to_csv(filepath, index=False)
    print(f"Audit data saved to {filepath}")

def create_html_graph(data, cluster_id):
    df = pd.DataFrame(data)

    # Create subplots: use 'domain' type for Pie subplot
    fig = make_subplots(rows=2, cols=2,
                        specs=[[{'type':'domain'}, {'type':'domain'}],
                               [{'colspan': 2}, None]],
                        subplot_titles=('GPU Distribution', 'Job Count Distribution', 'User Statistics'))

    # Add pie charts
    fig.add_trace(go.Pie(labels=df['user'], values=df['totalRequestedGPUs'], name="GPUs"),
                  1, 1)
    fig.add_trace(go.Pie(labels=df['user'], values=df['jobCount'], name="Jobs"),
                  1, 2)

    # Add bar chart
    fig.add_trace(go.Bar(x=df['user'], y=df['totalRequestedGPUs'], name='Total Requested GPUs', marker_color='#1f77b4'),
                  2, 1)
    fig.add_trace(go.Bar(x=df['user'], y=df['jobCount'], name='Job Count', marker_color='#ff7f0e'),
                  2, 1)

    # Update layout
    fig.update_layout(
        title=f'Job Statistics for Cluster {cluster_id}',
        height=800,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        annotations=[dict(text="GPUs per User", x=0.225, y=0.8, font_size=14, showarrow=False),
                     dict(text="Jobs per User", x=0.775, y=0.8, font_size=14, showarrow=False)],
        barmode='group'
    )

    # Update xaxes
    fig.update_xaxes(title_text="User", row=2, col=1)

    # Update yaxes
    fig.update_yaxes(title_text="Count", row=2, col=1)

    audit_folder = 'audit'
    if not os.path.exists(audit_folder):
        os.makedirs(audit_folder)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"job_audit_{cluster_id}_{timestamp}.html"
    filepath = os.path.join(audit_folder, filename)

    fig.write_html(filepath, full_html=False, include_plotlyjs='cdn')
    print(f"HTML graph saved to {filepath}")

def main():
    parser = argparse.ArgumentParser(description="Fetch Run:AI job information")
    parser.add_argument("--cluster", required=True, help="Cluster ID")
    args = parser.parse_args()

    token = login()

    result = get_job_information(token, args.cluster)
    if result:
        # Check if result is a list (of jobs) or a dict containing a 'jobs' key
        jobs = result if isinstance(result, list) else result.get('jobs', [])
        if not jobs:
            print("No jobs found in the API response.")
            return

        consolidated_jobs = consolidate_jobs(jobs)

        print("Consolidated Jobs:")
        print(json.dumps(consolidated_jobs, indent=2))

        save_to_csv(consolidated_jobs, args.cluster)
        create_html_graph(consolidated_jobs, args.cluster)
        print("Script is working")
    else:
        print("Script is not working")

if __name__ == "__main__":
    main()
