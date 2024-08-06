import requests
import os
import psycopg2
from git import Repo
from variables import GITLAB_URL, PERSONAL_ACCESS_TOKEN, DB_CONFIG, BACKUP_DIR  # Importing variables

# Dictionary containing authentication header
headers = {'PRIVATE-TOKEN': PERSONAL_ACCESS_TOKEN}

# Function to connect to PostgreSQL
def get_db_connection():
    conn = psycopg2.connect(
        dbname=DB_CONFIG['dbname'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port']
    )
    return conn

# Function to create tables if they don't exist
def create_tables(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id BIGINT PRIMARY KEY,
            username TEXT,
            name TEXT,
            state TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS projects (
            id BIGINT PRIMARY KEY,
            name TEXT,
            namespace TEXT,
            http_url_to_repo TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS pipelines (
            id BIGINT PRIMARY KEY,
            project_id BIGINT,
            ref TEXT,
            status TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );
        """)
        conn.commit()

# Function to insert user data into PostgreSQL
def insert_users(conn, users):
    with conn.cursor() as cursor:
        for user in users:
            cursor.execute("""
            INSERT INTO users (id, username, name, state, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
            """, (user['id'], user['username'], user['name'], user['state'], user['created_at'], user['updated_at']))
        conn.commit()

# Function to insert project data into PostgreSQL
def insert_projects(conn, projects):
    with conn.cursor() as cursor:
        for project in projects:
            cursor.execute("""
            INSERT INTO projects (id, name, namespace, http_url_to_repo, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
            """, (project['id'], project['name'], project['namespace']['full_path'], project['http_url_to_repo'], project['created_at'], project['updated_at']))
        conn.commit()

# Function to insert pipeline data into PostgreSQL
def insert_pipelines(conn, pipelines, project_id):
    with conn.cursor() as cursor:
        for pipeline in pipelines:
            cursor.execute("""
            INSERT INTO pipelines (id, project_id, ref, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
            """, (pipeline['id'], project_id, pipeline['ref'], pipeline['status'], pipeline['created_at'], pipeline['updated_at']))
        conn.commit()

def fetch_users():
    response = requests.get(f'{GITLAB_URL}/api/v4/users', headers=headers)
    response.raise_for_status()
    return response.json()

def fetch_projects():
    projects = []
    page = 1
    while True:
        response = requests.get(f'{GITLAB_URL}/api/v4/projects', headers=headers, params={'page': page, 'per_page': 100})
        response.raise_for_status()
        page_projects = response.json()
        if not page_projects:
            break
        projects.extend(page_projects)
        page += 1
    return projects

def clone_repository(repo_url, clone_dir):
    if not os.path.exists(clone_dir):
        os.makedirs(clone_dir)
    Repo.clone_from(repo_url, clone_dir)

def main():
    conn = get_db_connection()
    create_tables(conn)

    # Fetch and insert Users
    users = fetch_users()
    insert_users(conn, users)

    # Fetch and insert Projects
    projects = fetch_projects()
    insert_projects(conn, projects)

    for project in projects:
        project_id = project['id']
        project_name = project['name']
        namespace = project['namespace']['full_path']
        clone_url = project['http_url_to_repo']
        clone_dir = os.path.join(BACKUP_DIR, 'repositories', namespace, project_name)

        print(f'Cloning repository: {clone_url} to {clone_dir}')
        clone_repository(clone_url, clone_dir)

        # Fetch and insert pipeline scripts
        pipelines_response = requests.get(f'{GITLAB_URL}/api/v4/projects/{project_id}/pipelines', headers=headers)
        pipelines_response.raise_for_status()
        pipelines = pipelines_response.json()
        insert_pipelines(conn, pipelines, project_id)

    conn.close()

if __name__ == "__main__":
    main()
