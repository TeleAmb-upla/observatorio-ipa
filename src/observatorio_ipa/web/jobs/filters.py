import django_filters
from django.db.models import Q
from .models import Job, Export


# Filter definition for running jobs
class RunningJobsFilter(django_filters.FilterSet):
    created_at = django_filters.DateFromToRangeFilter()
    updated_at = django_filters.DateFromToRangeFilter()

    class Meta:
        model = Job
        fields = ["created_at", "updated_at"]


class ExportFilter(django_filters.FilterSet):
    export_search = django_filters.CharFilter(method="filter_search", label="Search")
    state = django_filters.CharFilter(field_name="state", lookup_expr="exact")
    type = django_filters.CharFilter(field_name="type", lookup_expr="exact")

    class Meta:
        model = Export
        fields = ["export_search", "state", "type"]

    def filter_search(self, queryset, name, value):
        return queryset.filter(Q(id__icontains=value) | Q(name__icontains=value))
