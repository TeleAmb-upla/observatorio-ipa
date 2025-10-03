import django_filters
from .models import Job


# Filter definition for running jobs
class RunningJobsFilter(django_filters.FilterSet):
    created_at = django_filters.DateFromToRangeFilter()
    updated_at = django_filters.DateFromToRangeFilter()

    class Meta:
        model = Job
        fields = ["created_at", "updated_at"]
