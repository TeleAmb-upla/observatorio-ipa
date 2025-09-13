# Table definition for running jobs
from django.utils.safestring import mark_safe
from django.utils.html import format_html
from django.urls import reverse
import django_tables2 as tables
from .models import Job, Export


def status_color(status):
    """Return a color class based on job status."""
    if status == "COMPLETED":
        return "table-success"
    elif status == "FAILED":
        return "table-danger"
    elif status == "RUNNING":
        return "table-secondary"
    return ""


class StatusColumn(tables.Column):
    attrs = {"td": {"class": lambda value: status_color(value)}}


class JobsTable(tables.Table):
    job_status = StatusColumn(orderable=True)
    created_at = tables.Column(orderable=True)
    updated_at = tables.Column(orderable=True)

    # def render_job_status(self, value):
    #     color_class = ""
    #     if value == "COMPLETED":
    #         color_class = "bg-success text-white"
    #     elif value == "FAILED":
    #         color_class = "bg-danger text-white"
    #     elif value == "RUNNING":
    #         color_class = "bg-secondary text-white"
    #     if color_class:
    #         return mark_safe(
    #             f'<span class="{color_class}" style="display:inline-block; padding:0.25em 0.75em; border-radius:1em; font-weight:500;">{value}</span>'
    #         )
    #     return value

    def render_id(self, value):
        url = reverse("job_detail", args=[value])
        return format_html(
            f'<a class="fw-semibold text-decoration-none" href="{url}">{value} <i class="bi bi-box-arrow-up-right" aria-hidden="true"></i></a>'
        )

    class Meta:
        model = Job
        template_name = "django_tables2/bootstrap5.html"
        attrs = {"class": "table osn-table"}
        fields = ("job_status", "id", "created_at", "updated_at")
        order_by = ("-created_at",)


class ExportsTable(tables.Table):
    type = tables.Column(orderable=True)
    name = tables.Column(orderable=True)
    state = StatusColumn(orderable=True)
    target = tables.Column(orderable=True)
    created_at = tables.Column(orderable=True)
    updated_at = tables.Column(orderable=True)

    def render_name(self, record):
        url = reverse("export_detail", args=[record.id])
        return format_html(
            f'<a class="fw-semibold text-decoration-none " href="{url}">{record.name} <i class="bi bi-box-arrow-up-right" aria-hidden="true"></i></a>'
        )

    class Meta:
        model = Export
        template_name = "django_tables2/bootstrap5.html"
        attrs = {"class": "table osn-table"}
        fields = ("state", "type", "name", "target", "created_at", "updated_at")
        order_by = (
            "type",
            "-created_at",
        )
