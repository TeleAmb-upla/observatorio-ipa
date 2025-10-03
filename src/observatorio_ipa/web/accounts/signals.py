from django.db.models.signals import post_save
from django.dispatch import receiver
from django.conf import settings
from allauth.account.models import EmailAddress
from .models import User

@receiver(post_save, sender=User)
def create_emailaddress_for_user(sender, instance, created, **kwargs):
    if created and instance.email:
        # Check if EmailAddress already exists
        if not EmailAddress.objects.filter(user=instance, email=instance.email).exists():
            EmailAddress.objects.create(
                user=instance,
                email=instance.email,
                verified=False,  # Set to True if you want to auto-verify
                primary=True
            )

@receiver(post_save, sender=EmailAddress)
def sync_user_email(sender, instance, **kwargs):
    if instance.primary:
        user = instance.user
        if user.email != instance.email:
            user.email = instance.email
            user.save()
