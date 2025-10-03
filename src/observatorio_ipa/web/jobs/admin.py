from django.contrib import admin
from .models import Job, Export, FileTransfer, Modis, Report, WebsiteUpdate

# Register your models here.


class JobAdmin(admin.ModelAdmin):
    list_display = ("id", "job_status", "created_at", "updated_at")


class ExportAdmin(admin.ModelAdmin):
    list_display = ("id", "job", "type", "state", "created_at", "updated_at")


class ModisAdmin(admin.ModelAdmin):
    list_display = ("name", "job_id")


admin.site.register(Job, JobAdmin)
admin.site.register(Export, ExportAdmin)
admin.site.register(FileTransfer)
admin.site.register(Modis, ModisAdmin)
admin.site.register(Report)
admin.site.register(WebsiteUpdate)
