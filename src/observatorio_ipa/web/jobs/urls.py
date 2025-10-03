# pages/urls.py
from django.urls import path
from .views import (
    JobListView,
    JobDetailView,
    ExportDetailView,
    RunningJobsListView,
    SearchJobsExportsView,
)

urlpatterns = [
    path("", RunningJobsListView.as_view(), name="home"),
    path("jobs/", JobListView.as_view(), name="job_list"),
    path("jobs/<str:pk>/", JobDetailView.as_view(), name="job_detail"),
    path("exports/<str:pk>/", ExportDetailView.as_view(), name="export_detail"),
    path("search/", SearchJobsExportsView.as_view(), name="search_jobs_exports"),
]
