import gitlab
import os
import subprocess

# Configuration
GITLAB_URL = 'https://your-gitlab-instance.com'
PRIVATE_TOKEN = 'your_private_token'
BACKUP_DIR = '/path/to/backup'

# Initialize GitLab connection
gl = gitlab.Gitlab(GITLAB_URL, private_token=PRIVATE_TOKEN)
gl.auth()

# Ensure backup directory exists
if not os.path.exists(BACKUP_DIR):
    os.makedirs(BACKUP_DIR)

def backup_repository(repo_url, project_name):
    repo_dir = os.path.join(BACKUP_DIR, project_name)
    if os.path.exists(repo_dir):
        # If the repo already exists, pull the latest changes
        subprocess.run(['git', '-C', repo_dir, 'pull'])
    else:
        # Clone the repo if it doesn't exist
        subprocess.run(['git', 'clone', repo_url, repo_dir])

# Get all projects
projects = gl.projects.list(all=True)
for project in projects:
    project_id = project.id
    project_name = project.path_with_namespace
    repo_url = project.ssh_url_to_repo

    print(f"Backing up {project_name} from {repo_url}")
    backup_repository(repo_url, project_name)

print("Backup complete.")
