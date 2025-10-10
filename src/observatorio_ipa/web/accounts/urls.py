from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.http import HttpResponseRedirect
from django.urls import reverse
from allauth.account.views import (
    SignupView,
    LoginView,
    LogoutView,
    PasswordChangeView,
    EmailView,
)
from allauth.socialaccount.providers.github import urls as github_urls
from allauth.socialaccount.providers.google import urls as google_urls
from .views import PasswordSetDisabledView


# Create a simple redirect view for disabled signup
def signup_disabled(request) -> HttpResponseRedirect:
    """Redirect signup attempts to login page since signup is disabled."""
    return HttpResponseRedirect(reverse("account_login"))


urlpatterns = [
    # Basic account views
    path("login/", LoginView.as_view(), name="account_login"),
    path("logout/", LogoutView.as_view(), name="account_logout"),
    path(
        "password/change/", PasswordChangeView.as_view(), name="account_change_password"
    ),
    path("password/set/", PasswordSetDisabledView.as_view(), name="account_set_password"),
    # Disabled signup - redirect to login
    path("signup/", signup_disabled, name="account_signup"),
    # Social account URLs (includes login/cancelled/, login/error/, signup/, connections)
    path("3rdparty/", include("allauth.socialaccount.urls")),
]

# Add OAuth provider URLs only if they are enabled
if getattr(settings, "GITHUB_OAUTH_ENABLED", False):
    urlpatterns += [
        path("", include(github_urls)),
    ]

if getattr(settings, "GCP_OAUTH_ENABLED", False):
    urlpatterns += [
        path("", include(google_urls)),
    ]
