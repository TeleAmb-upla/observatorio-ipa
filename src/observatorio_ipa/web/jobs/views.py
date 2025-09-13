from django.views.generic import TemplateView, ListView, DetailView, View
from django.contrib.auth.mixins import LoginRequiredMixin
from django.shortcuts import render
from django_tables2.views import SingleTableMixin, RequestConfig
from django_filters.views import FilterView

from .models import Job, Export, FileTransfer, Report, WebsiteUpdate, Modis
from django.db.models import Q
from .tables import JobsTable, ExportsTable
from .filters import RunningJobsFilter


class RunningJobsListView(LoginRequiredMixin, SingleTableMixin, ListView):
    model = Job
    table_class = JobsTable
    template_name = "jobs/running_jobs.html"
    paginate_by = 10

    def get_queryset(self):
        queryset = Job.objects.filter(job_status="RUNNING")
        search_query = self.request.GET.get("running_job_search", "").strip()
        if search_query:
            queryset = queryset.filter(Q(id__icontains=search_query))
        return queryset

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["running_job_search_query"] = self.request.GET.get(
            "running_job_search", ""
        ).strip()
        return context


class JobListView(LoginRequiredMixin, SingleTableMixin, ListView):
    model = Job
    table_class = JobsTable
    template_name = "jobs/job_list.html"
    paginate_by = 10

    def get_queryset(self):
        queryset = super().get_queryset()
        search_query = self.request.GET.get("job_search", "").strip()
        if search_query:
            queryset = queryset.filter(Q(id__icontains=search_query))
        return queryset

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["job_search_query"] = self.request.GET.get("job_search", "").strip()
        return context


class JobDetailView(LoginRequiredMixin, DetailView):
    model = Job
    context_object_name = "job"
    template_name = "jobs/job_detail.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        # Searchbar for exports
        search_query = self.request.GET.get("export_search", "").strip()
        exports_qs = self.object.exports.all()  # type: ignore
        if search_query:
            exports_qs = exports_qs.filter(
                Q(id__icontains=search_query) | Q(name__icontains=search_query)
            )
        table = ExportsTable(exports_qs)
        RequestConfig(self.request, paginate={"per_page": 10}).configure(table)  # type: ignore
        context["exports_table"] = table
        context["export_search_query"] = search_query
        context["exports_empty"] = exports_qs.count() == 0

        # Split job.error by '|', strip whitespace, ignore empty
        error_str = getattr(self.object, "error", "")  # type: ignore
        if error_str:
            context["error_list"] = [
                e.strip() for e in error_str.split("|") if e.strip()
            ]
        else:
            context["error_list"] = []
        return context


class ExportDetailView(LoginRequiredMixin, DetailView):
    model = Export
    context_object_name = "export"
    template_name = "jobs/export_detail.html"


# Search view for Jobs/Exports by partial UUID
class SearchJobsExportsView(LoginRequiredMixin, View):
    def get(self, request):
        query = request.GET.get("q", "").strip()
        results = []
        if query:
            jobs = Job.objects.filter(id__icontains=query)
            exports = Export.objects.filter(id__icontains=query)
            for job in jobs:
                results.append({"type": "Job", "id": str(job.id)})
            for export in exports:
                results.append({"type": "Export", "id": str(export.id)})
        return render(
            request, "jobs/search_results.html", {"results": results, "query": query}
        )
