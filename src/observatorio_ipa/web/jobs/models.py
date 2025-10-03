# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from uuid import uuid4
from django.db import models
from django.core.exceptions import ValidationError


def make_uuid():
    return str(uuid4())


class Job(models.Model):
    JOB_STATUS_CHOICES = [
        ("RUNNING", "RUNNING"),
        ("COMPLETED", "COMPLETED"),
        ("FAILED", "FAILED"),
    ]
    STATUS_CHOICES = [
        ("PENDING", "PENDING"),
        ("RUNNING", "RUNNING"),
        ("COMPLETED", "COMPLETED"),
        ("FAILED", "FAILED"),
    ]

    id = models.CharField(
        primary_key=True,
        max_length=36,
        default=make_uuid,
        editable=False,
        verbose_name="Job",
    )
    job_status = models.CharField(
        max_length=32, choices=JOB_STATUS_CHOICES, verbose_name="Status"
    )
    image_export_status = models.CharField(
        max_length=32,
        default="PENDING",
        choices=STATUS_CHOICES,
        verbose_name="Image Export Status",
    )
    stats_export_status = models.CharField(
        max_length=32,
        default="PENDING",
        choices=STATUS_CHOICES,
        verbose_name="Stats Export Status",
    )
    website_update_status = models.CharField(
        max_length=32,
        default="PENDING",
        choices=STATUS_CHOICES,
        verbose_name="Website Update Status",
    )
    report_status = models.CharField(
        max_length=32,
        default="PENDING",
        choices=STATUS_CHOICES,
        verbose_name="Report Status",
    )
    error = models.TextField(blank=True, null=True, verbose_name="Error Messages")
    timezone = models.CharField(max_length=32, default="UTC", verbose_name="Timezone")
    created_at = models.DateTimeField(verbose_name="Created At")
    updated_at = models.DateTimeField(verbose_name="Updated At")

    class Meta:
        managed = False
        app_label = "jobs"
        db_table = "jobs"


class Export(models.Model):
    EXPORT_STATUS_CHOICES = [
        ("RUNNING", "RUNNING"),
        ("COMPLETED", "COMPLETED"),
        ("FAILED", "FAILED"),
        ("TIMED_OUT", "TIMED_OUT"),
    ]
    EXPORT_TYPE_CHOICES = [
        ("image", "Image"),
        ("table", "Table"),
    ]

    EXPORT_TARGET_CHOICES = [
        ("gee", "Google Earth Engine"),
        ("gdrive", "Google Drive"),
        ("storage", "Google Cloud Storage"),
    ]

    id = models.CharField(
        primary_key=True,
        max_length=36,
        default=make_uuid,
        editable=False,
        verbose_name="Export",
    )
    job = models.ForeignKey(
        "Job", models.CASCADE, db_column="job_id", related_name="exports"
    )
    state = models.CharField(
        max_length=32, choices=EXPORT_STATUS_CHOICES, verbose_name="Status"
    )
    type = models.CharField(max_length=32, choices=EXPORT_TYPE_CHOICES)
    name = models.CharField(max_length=255)
    target = models.CharField(max_length=255, choices=EXPORT_TARGET_CHOICES)
    path = models.TextField()
    task_id = models.CharField(
        max_length=255, blank=True, null=True, verbose_name="GEE Task ID"
    )
    task_status = models.CharField(
        max_length=32, blank=True, null=True, verbose_name="GEE Task Status"
    )
    error = models.TextField(blank=True, null=True)
    next_check_at = models.DateTimeField()
    lease_until = models.DateTimeField(blank=True, null=True)
    poll_interval_sec = models.IntegerField(default=5)
    attempts = models.IntegerField(default=0)
    deadline_at = models.DateTimeField(blank=True, null=True)
    created_at = models.DateTimeField()
    updated_at = models.DateTimeField()

    def clean(self):
        super().clean()
        if (
            self.next_check_at
            and self.created_at
            and self.next_check_at < self.created_at
        ):
            raise ValidationError(
                {"next_check_at": "next_check_at cannot be earlier than created_at."}
            )

    class Meta:
        managed = False
        app_label = "jobs"
        db_table = "exports"


class FileTransfer(models.Model):
    TRANSFER_STATUS_CHOICES = [
        ("MOVED", "Moved"),
        ("NOT_MOVED", "Not Moved"),
        ("ROLLED_BACK", "Rolled Back"),
    ]

    id = models.AutoField(primary_key=True)
    job = models.ForeignKey(
        "Job", models.CASCADE, db_column="job_id", related_name="file_transfers"
    )
    export = models.OneToOneField(
        Export, models.CASCADE, db_column="export_id", related_name="file_transfers"
    )
    source_path = models.TextField(verbose_name="Source File")
    destination_path = models.TextField(verbose_name="Destination File")
    status = models.CharField(max_length=32, choices=TRANSFER_STATUS_CHOICES)
    created_at = models.DateTimeField()
    updated_at = models.DateTimeField()

    class Meta:
        managed = False
        app_label = "jobs"
        db_table = "file_transfers"


class WebsiteUpdate(models.Model):
    STATUS_CHOICES = [
        ("PENDING", "Pending"),
        ("COMPLETED", "Completed"),
        ("FAILED", "Failed"),
    ]
    id = models.AutoField(primary_key=True)
    job = models.OneToOneField(
        Job, models.CASCADE, db_column="job_id", related_name="website_updates"
    )
    status = models.CharField(max_length=32, choices=STATUS_CHOICES, default="PENDING")
    pull_request_id = models.CharField(max_length=255, blank=True, null=True)
    pull_request_url = models.CharField(max_length=255, blank=True, null=True)
    attempts = models.IntegerField(default=0)
    last_error = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField()
    updated_at = models.DateTimeField()

    class Meta:
        managed = False
        app_label = "jobs"
        db_table = "website_updates"


class Report(models.Model):
    STATUS_CHOICES = [
        ("PENDING", "Pending"),
        ("COMPLETED", "Completed"),
        ("FAILED", "Failed"),
    ]
    id = models.AutoField(primary_key=True)
    job = models.OneToOneField(
        Job, models.CASCADE, db_column="job_id", related_name="reports"
    )
    status = models.CharField(max_length=32, choices=STATUS_CHOICES)
    attempts = models.IntegerField(default=0)
    last_error = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField()
    updated_at = models.DateTimeField()

    class Meta:
        managed = False
        app_label = "jobs"
        db_table = "reports"


class Modis(models.Model):
    NAME_CHOICES = [("terra", "Terra"), ("aqua", "Aqua")]
    id = models.AutoField(primary_key=True)
    job = models.ForeignKey(
        Job, models.CASCADE, db_column="job_id", related_name="modis_entries"
    )
    name = models.CharField(max_length=255, choices=NAME_CHOICES)
    collection = models.CharField(max_length=255)
    images = models.IntegerField()
    last_image = models.CharField(max_length=255)
    created_at = models.DateTimeField()
    updated_at = models.DateTimeField()

    class Meta:
        managed = False
        app_label = "jobs"
        db_table = "modis"
