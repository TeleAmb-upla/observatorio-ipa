from django.views.generic import TemplateView, ListView, DetailView, View
from django.contrib.auth.mixins import LoginRequiredMixin
from django.shortcuts import render
from django_tables2.views import SingleTableMixin, RequestConfig
from django_filters.views import FilterView

from .models import Job, Export, FileTransfer, Report, WebsiteUpdate, Modis
from django.db.models import Q
from .tables import JobsTable, ExportsTable
from .filters import RunningJobsFilter


PAGINATION_SIZES = [10, 25, 50, 100]


class RunningJobsListView(LoginRequiredMixin, SingleTableMixin, ListView):
    model = Job
    table_class = JobsTable
    template_name = "jobs/running_jobs.html"
    paginate_by = 10

    def get_queryset(self):
        queryset = Job.objects.filter(job_status="RUNNING").order_by("-created_at")
        search_query = self.request.GET.get("running_job_search", "").strip()
        created_at_query = self.request.GET.get("running_job_created_at", "").strip()
        if search_query:
            queryset = queryset.filter(Q(id__icontains=search_query))
        if created_at_query:
            queryset = queryset.filter(created_at__date=created_at_query)
        return queryset

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["running_job_search_query"] = self.request.GET.get(
            "running_job_search", ""
        ).strip()
        context["running_job_created_at_query"] = self.request.GET.get(
            "running_job_created_at", ""
        ).strip()
        return context


class JobListView(LoginRequiredMixin, SingleTableMixin, ListView):
    model = Job
    table_class = JobsTable
    template_name = "jobs/job_list.html"
    paginate_by = 10

    def get_queryset(self):
        queryset = super().get_queryset().order_by("-created_at")
        search_query = self.request.GET.get("job_search", "").strip()
        created_at_query = self.request.GET.get("job_created_at", "").strip()
        job_status_selected = self.request.GET.get("job_status", "").strip()
        if search_query:
            queryset = queryset.filter(Q(id__icontains=search_query))
        if created_at_query:
            queryset = queryset.filter(created_at__date=created_at_query)
        if job_status_selected:
            queryset = queryset.filter(job_status=job_status_selected)
        return queryset

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["job_search_query"] = self.request.GET.get("job_search", "").strip()
        context["job_created_at_query"] = self.request.GET.get(
            "job_created_at", ""
        ).strip()
        context["job_status_selected"] = self.request.GET.get("job_status", "").strip()
        # Provide all possible job status choices for the dropdown
        context["job_status_choices"] = ["COMPLETED", "FAILED", "RUNNING"]
        return context


class JobDetailView(LoginRequiredMixin, DetailView):
    model = Job
    context_object_name = "job"
    template_name = "jobs/job_detail.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        # Export search and filters
        search_query = self.request.GET.get("export_search", "").strip()
        status_filter = self.request.GET.get("export_status", "").strip()
        type_filter = self.request.GET.get("export_type", "").strip()
        per_page = self.request.GET.get("per_page", "10")
        try:
            per_page = int(per_page)
        except ValueError:
            per_page = 10

        exports_qs = self.object.exports.all()  # type: ignore
        if search_query:
            exports_qs = exports_qs.filter(
                Q(id__icontains=search_query) | Q(name__icontains=search_query)
            )
        if status_filter:
            exports_qs = exports_qs.filter(state=status_filter)
        if type_filter:
            exports_qs = exports_qs.filter(type=type_filter)

        table = ExportsTable(exports_qs)
        RequestConfig(self.request, paginate={"per_page": per_page}).configure(table)  # type: ignore
        context["exports_table"] = table
        context["export_search_query"] = search_query
        context["exports_empty"] = exports_qs.count() == 0
        context["export_status_selected"] = status_filter
        context["export_type_selected"] = type_filter
        context["per_page"] = per_page
        context["per_page_choices"] = PAGINATION_SIZES

        # Choices for dropdowns (from all exports for this job)
        all_exports = self.object.exports.all()  # type: ignore
        context["export_status_choices"] = list(
            all_exports.values_list("state", flat=True).distinct()
        )
        context["export_type_choices"] = list(
            all_exports.values_list("type", flat=True).distinct()
        )

        # Export completion stats by type (dynamic, not hardcoded)
        export_stats = []
        export_types = all_exports.values_list("type", flat=True).distinct()
        for export_type in export_types:
            type_exports = all_exports.filter(type=export_type)
            n_exports = type_exports.count()
            n_completed = type_exports.exclude(state="RUNNING").count()
            pct_completed = (
                round((n_completed / n_exports) * 100) if n_exports > 0 else 0
            )
            export_stats.append(
                {
                    "type": export_type,
                    "n_exports": n_exports,
                    "n_completed": n_completed,
                    "pct_completed": pct_completed,
                }
            )
        context["export_completion_summary"] = export_stats

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
