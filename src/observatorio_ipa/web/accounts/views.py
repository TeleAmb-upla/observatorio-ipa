from django.contrib.auth.decorators import login_required
from django.http import HttpResponseNotAllowed
from django.shortcuts import render
from django.utils.decorators import method_decorator
from django.views.generic import TemplateView

# Create your views here.

# TODO create a custom admin-only user creation view


@method_decorator(login_required, name="dispatch")
class PasswordSetDisabledView(TemplateView):
    """
    Custom view that displays a message indicating password setting is disabled.

    This view completely disables password setting functionality for security reasons.
    It only renders a template explaining the policy and does not process any forms.
    """

    template_name = "account/password_set.html"

    def post(self, request, *args, **kwargs):
        """
        Block all POST requests to prevent password setting attempts.

        Returns HTTP 405 Method Not Allowed for any POST requests.
        This prevents malicious users from submitting forms even if they
        craft their own POST requests to this endpoint.
        """
        return HttpResponseNotAllowed(["GET"])

    def put(self, request, *args, **kwargs):
        """Block PUT requests."""
        return HttpResponseNotAllowed(["GET"])

    def patch(self, request, *args, **kwargs):
        """Block PATCH requests."""
        return HttpResponseNotAllowed(["GET"])

    def delete(self, request, *args, **kwargs):
        """Block DELETE requests."""
        return HttpResponseNotAllowed(["GET"])
