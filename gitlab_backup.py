import requests
import os
import json
#Repo is a class from gitpython module to clone repositories
from git import Repo
#Importing the variables that include login credentials for gitlab
from variables import GITLAB_URL, PERSONAL_ACCESS_TOKEN, BACKUP_DIR  # Importing the variables
import certifi

#dictionary containing authentication header
headers = {'PRIVATE-TOKEN': PERSONAL_ACCESS_TOKEN}

#Function for fetching user details in json format
def fetch_users():
    response = requests.get(f'{GITLAB_URL}/api/v4/users', headers=headers)
    response.raise_for_status()
    return response.json()

#function for fetching projects
def fetch_projects():
    projects = []
    page = 1
    #loop to check for projects in each page
    while True:
        response = requests.get(f'{GITLAB_URL}/api/v4/projects', headers=headers, params={'page': page, 'per_page': 100})
        response.raise_for_status()
        page_projects = response.json()
        if not page_projects:
            break
        projects.extend(page_projects)
        page += 1
    return projects

#function to clone repositories
def clone_repository(repo_url, clone_dir):
    if not os.path.exists(clone_dir):
        os.makedirs(clone_dir)
    Repo.clone_from(repo_url, clone_dir)

#function to save the data in a json file
def save_data(data, filename):
    with open(os.path.join(BACKUP_DIR, filename), 'w') as f:
        json.dump(data, f, indent=4)

def main():
    if not os.path.exists(BACKUP_DIR):
        os.makedirs(BACKUP_DIR)

    #Fetches and Backup Users
    users = fetch_users()
    save_data(users, 'users.json')

    #Fetches Backup Projects
    projects = fetch_projects()
    save_data(projects, 'projects.json')

    for project in projects:
        project_id = project['id']
        project_name = project['name']
        namespace = project['namespace']['full_path']
        clone_url = project['http_url_to_repo']
        clone_dir = os.path.join(BACKUP_DIR, 'repositories', namespace, project_name)

        print(f'Cloning repository: {clone_url} to {clone_dir}')
        clone_repository(clone_url, clone_dir)

        # Fetch and save pipeline scripts
        pipelines_response = requests.get(f'{GITLAB_URL}/api/v4/projects/{project_id}/pipelines', headers=headers)
        pipelines_response.raise_for_status()
        pipelines = pipelines_response.json()
        save_data(pipelines, f'{namespace}_{project_name}_pipelines.json')

if __name__ == "__main__":
    main()
