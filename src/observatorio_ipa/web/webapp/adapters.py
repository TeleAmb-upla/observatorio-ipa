import logging
import requests
from allauth.socialaccount.adapter import DefaultSocialAccountAdapter
from allauth.socialaccount.models import SocialLogin
from allauth.core.exceptions import ImmediateHttpResponse
from allauth.account.models import EmailAddress
from django.http import HttpResponseForbidden, HttpRequest
from django.contrib import messages
from django.conf import settings
from django.contrib.auth import get_user_model
from django.shortcuts import redirect
from google.oauth2 import credentials
from google.cloud import resourcemanager_v3

logger = logging.getLogger("osn-ipa")


class CustomSocialAccountAdapter(DefaultSocialAccountAdapter):
    """Custom adapter to restrict access to GCP project members and GitHub repository contributors."""

    def pre_social_login(self, request: HttpRequest, social_login: SocialLogin) -> None:
        """Check if user has access to the configured resources before allowing login. Fail gracefully if email exists."""

        email = social_login.account.extra_data.get("email")
        User = get_user_model()
        # If the email exists, redirect to Allauth's built-in error view
        if email and User.objects.filter(email=email).exists():
            messages.error(
                request,
                "Unable to log in with this social account.",
            )
            raise ImmediateHttpResponse(redirect("socialaccount_login_error"))

        provider = social_login.account.provider

        if provider == "google":
            self._check_gcp_access(request, social_login)
        elif provider == "github":
            self._check_github_access(request, social_login)
        else:
            logger.error(f"Unsupported OAuth provider: {provider}")
            messages.error(request, "Unsupported authentication provider.")
            raise ImmediateHttpResponse(HttpResponseForbidden("Unsupported provider"))

    def _check_gcp_access(
        self, request: HttpRequest, social_login: SocialLogin
    ) -> None:
        """Check if user has access to the GCP project."""
        project_id = getattr(settings, "GCP_PROJECT_ID", None)

        if not project_id:
            logger.error("GCP_PROJECT_ID not configured for OAuth authentication")
            messages.error(
                request, "GCP OAuth authentication is not properly configured."
            )
            raise ImmediateHttpResponse(
                HttpResponseForbidden("GCP OAuth not configured")
            )

        token = self._get_token(request, social_login)

        try:
            if not self._check_gcp_project_access(token, project_id):
                user_email = getattr(social_login.user, "email", "unknown")
                logger.warning(
                    f"Access denied for user {user_email} - not a GCP project member"
                )
                messages.error(
                    request,
                    "Access denied. You must be a member of the authorized GCP project.",
                )
                raise ImmediateHttpResponse(
                    HttpResponseForbidden("Not a GCP project member")
                )
        except Exception as e:
            logger.error(f"Error checking GCP project access: {str(e)}")
            messages.error(request, "Error validating GCP project access.")
            raise ImmediateHttpResponse(
                HttpResponseForbidden("GCP project access validation failed")
            )

    def _check_github_access(
        self, request: HttpRequest, social_login: SocialLogin
    ) -> None:
        """Check if user is a contributor to the specified GitHub repository."""
        repo_owner = getattr(settings, "GITHUB_REPOSITORY_OWNER", None)
        repo_name = getattr(settings, "GITHUB_REPOSITORY_NAME", None)

        if not repo_owner or not repo_name:
            logger.error("GitHub repository not configured for OAuth authentication")
            messages.error(
                request, "GitHub OAuth authentication is not properly configured."
            )
            raise ImmediateHttpResponse(
                HttpResponseForbidden("GitHub OAuth not configured")
            )

        token = self._get_token(request, social_login)

        try:
            if not self._check_github_repo_access(token, repo_owner, repo_name):
                user_login = getattr(social_login.user, "username", "unknown")
                logger.warning(
                    f"Access denied for user {user_login} - no access to repository"
                )
                messages.error(
                    request,
                    f"Access denied. You must have access to the {repo_owner}/{repo_name} repository.",
                )
                raise ImmediateHttpResponse(
                    HttpResponseForbidden("No repository access")
                )
        except Exception as e:
            logger.error(f"Error checking GitHub repository access: {str(e)}")
            messages.error(request, "Error validating GitHub repository access.")
            raise ImmediateHttpResponse(
                HttpResponseForbidden("GitHub repository access validation failed")
            )

    def _get_token(self, request: HttpRequest, social_login: SocialLogin) -> str:
        # Get user's access token
        if (
            not hasattr(social_login, "token")
            or social_login.token is None
            or not hasattr(social_login.token, "token")
        ):
            logger.error("Social login token is missing or invalid.")
            messages.error(request, "Authentication token missing or invalid.")
            raise ImmediateHttpResponse(HttpResponseForbidden("Invalid token"))

        return social_login.token.token

    def _check_gcp_project_access(self, token: str, project_id: str) -> bool:
        """Check if user has access to the specified GCP project using Cloud Client Library."""
        try:
            creds = credentials.Credentials(token)
            client = resourcemanager_v3.ProjectsClient(credentials=creds)
            project = client.get_project(name=f"projects/{project_id}")
            logger.info(
                f"User has access to project: {getattr(project, 'display_name', project_id)}"
            )
            return True
        except Exception as e:
            # PermissionDenied or NotFound means no access
            logger.error(f"Error checking project access: {e}")
            return False

    def _check_github_repo_access(
        self, token: str, repo_owner: str, repo_name: str
    ) -> bool:
        """Check if user has access to the specified GitHub repository."""
        try:
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github.v3+json",
                "User-Agent": "observatorio-ipa",
            }

            # Get authenticated user
            user_response = requests.get("https://api.github.com/user", headers=headers)
            if user_response.status_code != 200:
                logger.error(f"Failed to get user info: {user_response.status_code}")
                return False

            user_login = user_response.json().get("login")
            if not user_login:
                logger.error("Could not get user login from GitHub API")
                return False

            # Strategy 1: Check if user can access the repository directly
            # This works for both public repos and private repos the user has access to
            repo_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}"
            repo_response = requests.get(repo_url, headers=headers)

            if repo_response.status_code == 200:
                logger.info(
                    f"User {user_login} has read access to {repo_owner}/{repo_name}"
                )
                return True
            elif repo_response.status_code == 404:
                # Repository not found or user doesn't have access
                logger.info(
                    f"User {user_login} does not have access to {repo_owner}/{repo_name}"
                )
                return False
            else:
                logger.error(
                    f"Unexpected response when checking repository access: {repo_response.status_code}"
                )
                return False

        except requests.RequestException as e:
            logger.error(f"Request error checking GitHub repository access: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error checking GitHub repository access: {e}")
            return False
