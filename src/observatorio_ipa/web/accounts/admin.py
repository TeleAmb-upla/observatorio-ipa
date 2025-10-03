from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from allauth.account.models import EmailAddress
from .models import User

# Register your models here.


class EmailAddressInline(admin.TabularInline):
    model = EmailAddress
    extra = 0


class CustomUserAdmin(UserAdmin):
    inlines = [EmailAddressInline]


admin.site.register(User, CustomUserAdmin)
