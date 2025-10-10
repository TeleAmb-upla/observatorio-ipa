from django.conf import settings


def oauth_context(request):
    """Add OAuth configuration to template context."""
    return {
        "GCP_OAUTH_CLIENT_ID": getattr(settings, "GCP_OAUTH_CLIENT_ID", None),
        "GCP_OAUTH_ENABLED": bool(getattr(settings, "GCP_OAUTH_ENABLED", False)),
        "GITHUB_OAUTH_CLIENT_ID": getattr(settings, "GITHUB_OAUTH_CLIENT_ID", None),
        "GITHUB_OAUTH_ENABLED": bool(getattr(settings, "GITHUB_OAUTH_ENABLED", False)),
    }
