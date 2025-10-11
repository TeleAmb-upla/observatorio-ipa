# Usage

## Using Prefix

A prefix is required to read and save images. If no prefix is provided, the script will raise an error.

The prefix should be a string that ends with either an underscore "_" or a hyphen "-". If the provided prefix does not end with these characters, an underscore will be added to it. For example, if the provided prefix is "image", it will be changed to "image_".

## Cloud Storage

- Grant admin users "Storage Folder Admin" Role so they can manage everything within the bucket including creating folders 
- Grant object editors "Storage Object Admin" Role so they can create, edit and delete objects within the bucket
- Grant viewers "Storage Object Viewer" Role so they can view objects within the bucket

### Automatic Website Update

To automatically update the OSN website a Github App is required to push changes to the repository. The app must have the following permissions:

- Repository permissions
  - Contents - Read and Write
  - Metadata - Read-only
  - Pull requests - Read and Write

If required You can create a new Github App by following [GitHub's documentation](https://docs.github.com/en/apps/creating-github-apps/registering-a-github-app/registering-a-github-app)

The following App has already been created and owned by TeleAmb-upla. Create new Private Keys for any new installation of this application.

- Name: OSN Auto Website Update
- App ID: 2053829


### GCP Service Account

A GCP service account is required to run the application. The service account must have the following roles:

- Storage Object Admin: to be able to create, edit and delete objects within the GCP storage bucket
- Add GEE roles

### GCP Storage Bucket

A GCP storage bucket is required to store the tables exported from GEE as CSV files. Create a new bucket or use an existing one. The service account used by the application must have the "Storage Object Admin" role to be able to create, edit and delete objects within the bucket.

In order for the IPA application to automatically swap the csv files in the OSN website, the bucket must have a folder structure like the following:

```
gs://<your-bucket-name>/<any-folder-path>
    ├── elev/
    ├── month/
    ├── year/
    └── yearMonth/
    └── archive/
        ├── elev/
        ├── month/
        ├── year/
        └── yearMonth/
```

### New Docker installation

1. If using PostgreSQL, create a volume for the db

   ```bash
   docker volume create --name observatorio_ipa_db
   ```

2. Gather the necessary secrets.
    - GCP service account key: JSON key file for a service account with the necessary permissions (Required)
    - db user: Password for the database read-only user (Required)
    - db password: Password for the database admin user (Required)
    - Github app private key: PEM file for the private key of the Github App used to update the website (Required if using automatic website update)
    - SMTP user: User of the account used to send emails (Optional)
    - SMTP password: Password for the account used to send emails (Optional)
    - Django Secret Key: Secret key for OSN Automation monitoring web application (Optional, only required if deploying the web application)

3. Create or Gather the config files
    - toml config file: Configuration file for the Image Processing Application (Required)
    - web config file: Configuration file for the web application (Optional, only required if deploying the web application)

4. Create the necessary docker-compose.yml and config files. See examples in this repository for reference.
    - docker-compose.yml
    - ipa_config.toml
    - web_config.toml (Optional, only required if deploying the web application)

5. Create DB Schema and apply migrations
This has two options:
Option A: if using a postgres database, start the db service (container) and make the db accessible at to the host. Clone the repository and run the following commands:

```bash
ls
```

Option B: Run all three services (db, ipa, web). The IPA service will auto-create the DB schema if it does not exist. 
The Web service will require migrations to be applied manually. Connect to the web container and run the following commands:

```bash
cd /app/src/observatorio_ipa/web
python manage.py migrate accounts
python manage.py createsuperuser 
```

Follow the steps to create a superuser account to admin the Web application. Test the login at the web applications admin page: http://localhost:8000/admin or http://<your-domain>/admin
If all is working correctly you should be able to login. Once logged in you can log out exit the container.

## OAuth Access

The Web Interface supports OAuth authentication to provide secure access control. Users can authenticate with external providers while maintaining local account support as a fallback.

### GitHub OAuth Authentication

GitHub OAuth allows you to restrict access to users who have access to a specific repository. This provides automatic access control based on your project's existing GitHub permissions.

#### Prerequisites

Before configuring GitHub OAuth, you need to:

1. **Create a GitHub OAuth App** - Follow the [GitHub OAuth App creation guide](https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/creating-an-oauth-app)
2. **Configure OAuth App Settings**:
   - **Authorization callback URL**: `http://localhost:8000/accounts/github/login/callback/` (for development) or `https://yourdomain.com/accounts/github/login/callback/` (for production)
   - **Homepage URL**: Your application's URL

For detailed steps on creating OAuth credentials, refer to GitHub's official documentation: [Authorizing OAuth Apps](https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/authorizing-oauth-apps)

#### Configuration

Add the following section to your web configuration TOML file:

```toml
[github_oauth]
enable_github_oauth = true
oauth_client_id_file = "/path/to/secrets/github_oauth_client_id.txt"
oauth_client_secret_file = "/path/to/secrets/github_oauth_client_secret.txt"
repository_owner = "your-organization"
repository_name = "your-repository"
```

#### Configuration Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `enable_github_oauth` | boolean | Yes | Enable or disable GitHub OAuth authentication |
| `oauth_client_id_file` | string (file path) | Yes* | Path to file containing GitHub OAuth Client ID |
| `oauth_client_secret_file` | string (file path) | Yes* | Path to file containing GitHub OAuth Client Secret |
| `repository_owner` | string | Yes* | GitHub username or organization that owns the repository |
| `repository_name` | string | Yes* | Name of the repository to check access permissions |

*Required when `enable_github_oauth` is `true`

#### Access Control

When GitHub OAuth is enabled, the system will:

1. Authenticate users through GitHub OAuth
2. Verify the user has read access to the specified repository
3. Grant access to users who can view the repository (collaborators, contributors, organization members)
4. Deny access to users who cannot access the repository

**Note**: Users need read access to the repository, which includes:

- Repository collaborators (any permission level)
- Organization members with repository access
- Users who have contributed to public repositories
- Anyone for public repositories (if that's desired behavior)

#### Security Considerations

- **Repository Visibility**: The target repository can be public or private
- **Token Scope**: The application requests minimal scopes (`read:user`, `user:email`, `read:org`)
- **API Rate Limits**: GitHub API rate limits apply to contributor verification
- **Fallback Authentication**: Local user accounts remain available when OAuth is configured

#### Troubleshooting

Common issues and solutions:

- **"No repository access"**: Ensure the user has been added as a collaborator or has access through organization membership
- **"Repository not found"**: Verify `repository_owner` and `repository_name` are correct
- **OAuth configuration errors**: Check that client ID and secret files exist and contain valid credentials
